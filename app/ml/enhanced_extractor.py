"""
Enhanced Job Extraction Module
Handles multiple jobs per message, company extraction, improved pattern matching
"""

import re
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from sqlalchemy.orm import Session

from app.models.company import Company
from app.ml.base_classifier import ExtractionResult


@dataclass
class EnhancedJobExtraction:
    """Enhanced job extraction with all fields"""
    # Basic info
    job_title: Optional[str] = None
    company_name: Optional[str] = None
    company_id: Optional[str] = None
    
    # Job details
    location: Optional[str] = None
    location_data: Optional[Dict] = None  # Structured location intelligence
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    salary_currency: str = "INR"
    salary_raw: Optional[str] = None
    
    # Experience
    experience_min: Optional[int] = None
    experience_max: Optional[int] = None
    experience_raw: Optional[str] = None
    is_fresher_friendly: bool = False
    
    # Contact & Application
    apply_email: Optional[str] = None
    apply_link: Optional[str] = None
    contact_emails: List[str] = None
    contact_phones: List[str] = None
    
    # Skills & Requirements
    skills: List[str] = None
    description: str = ""
    
    # Metadata
    confidence: float = 0.0
    raw_text: str = ""
    
    def __post_init__(self):
        if self.contact_emails is None:
            self.contact_emails = []
        if self.contact_phones is None:
            self.contact_phones = []
        if self.skills is None:
            self.skills = []


class EnhancedJobExtractor:
    """Enhanced extraction with multiple job splitting and company management"""
    
    # India cities whitelist
    INDIA_CITIES = {
        'bangalore', 'bengaluru', 'mumbai', 'delhi', 'ncr', 'hyderabad', 'chennai',
        'kolkata', 'pune', 'ahmedabad', 'jaipur', 'lucknow', 'kanpur', 'nagpur',
        'indore', 'bhopal', 'noida', 'gurgaon', 'gurugram', 'chandigarh', 'kochi',
        'trivandrum', 'mysore', 'bhubaneswar', 'jamshedpur', 'ranchi', 'coimbatore',
        'vadodara', 'visakhapatnam', 'patna', 'ludhiana', 'agra', 'nashik',
        'faridabad', 'meerut', 'rajkot', 'varanasi', 'srinagar', 'aurangabad',
        'dhanbad', 'amritsar', 'allahabad', 'howrah', 'jabalpur', 'gwalior', 'india'
    }
    
    # International location keywords (from data analysis)
    INTERNATIONAL_KEYWORDS = {
        'usa', 'united states', 'america', 'us only', 'california', 'new york',
        'texas', 'washington', 'costa mesa', 'silicon valley',
        'uk', 'united kingdom', 'london', 'manchester', 'england',
        'canada', 'toronto', 'vancouver', 'montreal',
        'australia', 'sydney', 'melbourne',
        'singapore',
        'dubai', 'uae', 'abu dhabi',
        'europe', 'european', 'germany', 'berlin', 'france', 'paris',
        'netherlands', 'amsterdam', 'ireland', 'dublin', 'switzerland',
        'japan', 'china', 'israel',
        'overseas', 'international', 'gulf', 'abroad'
    }
    
    # Onsite-only indicators (from data analysis)
    ONSITE_ONLY_KEYWORDS = {
        'onsite only', 'on-site only', 'on site only', 'office only',
        'work from office', 'wfo mandatory', 'wfo only', 'office mandatory',
        'must work from office', 'no remote', 'no wfh', 'not remote',
        'in-office', 'in office', '100% office', 'fully office',
        'relocate', 'relocation required', 'must be based in',
        'local candidates only', 'relocation mandatory'
    }
    
    def __init__(self):
        self.company_cache = {}  # In-memory cache for session
    
    def extract_jobs_from_message(self, text: str, links: List[str] = None) -> List[EnhancedJobExtraction]:
        """
        Main entry point: Extract one or more jobs from a message
        
        Returns:
            List of EnhancedJobExtraction (one per job found)
        """
        # Step 1: Check if message contains multiple jobs
        job_sections = self._split_into_jobs(text)
        
        # Step 2: Extract from each section
        extractions = []
        for section in job_sections:
            extraction = self._extract_single_job(section, links)
            if extraction:
                extractions.append(extraction)
        
        return extractions
    
    def _split_into_jobs(self, text: str) -> List[str]:
        """
        Split message into individual job sections if multiple jobs present
        
        Patterns that indicate multiple jobs:
        - Numbered lists (1. 2. 3.)
        - Bullet points (•, -, *)
        - "Company X is hiring" repeated multiple times
        - Multiple "Apply here:" links
        """
        # Pattern 1: Numbered lists (1. Python Dev\n2. React Dev)
        numbered_pattern = r'\n(\d+[\.\)]\s+.+?)(?=\n\d+[\.\)]|\Z)'
        numbered_matches = re.findall(numbered_pattern, text, re.DOTALL)
        
        if len(numbered_matches) >= 2:
            return [match.strip() for match in numbered_matches]
        
        # Pattern 2: Multiple "Company is hiring" blocks
        company_hiring_pattern = r'([A-Z][A-Za-z0-9\s&\.]+?\s+is hiring.+?)(?=[A-Z][A-Za-z0-9\s&\.]+?\s+is hiring|\Z)'
        company_blocks = re.findall(company_hiring_pattern, text, re.DOTALL | re.IGNORECASE)
        
        if len(company_blocks) >= 2:
            return [block.strip() for block in company_blocks]
        
        # Pattern 3: Multiple apply links (each job has "Apply here:")
        apply_sections = re.split(r'Apply here:', text, flags=re.IGNORECASE)
        
        if len(apply_sections) > 2:  # First section is header, then job1, job2, etc.
            # Reconstruct sections with "Apply here:"
            reconstructed = []
            for i in range(1, len(apply_sections)):
                section = apply_sections[i-1].split('\n')[-3:] + ['Apply here:'] + [apply_sections[i]]
                reconstructed.append('\n'.join(section))
            return [s.strip() for s in reconstructed if s.strip()]
        
        # No multiple jobs detected, return full text
        return [text]
    
    def _extract_single_job(self, text: str, links: List[str] = None) -> Optional[EnhancedJobExtraction]:
        """Extract all information from a single job posting"""
        extraction = EnhancedJobExtraction()
        extraction.raw_text = text
        extraction.description = text
        
        # Extract each component
        extraction.company_name = self._extract_company(text)
        extraction.job_title = self._extract_job_title(text, extraction.company_name)
        
        # Extract location with enhanced intelligence
        location_data = self._extract_location_enhanced(text)
        extraction.location = location_data.get('raw_location')
        extraction.location_data = location_data
        
        # Salary
        salary_info = self._extract_salary(text)
        if salary_info:
            extraction.salary_min, extraction.salary_max, extraction.salary_raw = salary_info
        
        # Experience
        exp_info = self._extract_experience(text)
        if exp_info:
            extraction.experience_min, extraction.experience_max, extraction.experience_raw, extraction.is_fresher_friendly = exp_info
        
        # Contact info
        extraction.contact_emails = self._extract_emails(text)
        extraction.apply_email = extraction.contact_emails[0] if extraction.contact_emails else None
        extraction.contact_phones = self._extract_phones(text)
        
        # Apply link
        extraction.apply_link = self._extract_apply_link(text, links)
        
        # Skills
        extraction.skills = self._extract_skills(text)
        
        # Calculate confidence
        extraction.confidence = self._calculate_extraction_confidence(extraction)
        
        return extraction if extraction.confidence > 0.3 else None
    
    def _extract_company(self, text: str) -> Optional[str]:
        """Extract company name with multiple patterns"""
        patterns = [
            # "Adobe is hiring"
            r'^([A-Z][A-Za-z0-9\s&\.]{2,40}?)\s+is\s+hiring',
            
            # "Company: Google"
            r'(?:Company|Organization|Hiring\s+for)\s*:?\s*([A-Z][A-Za-z0-9\s&\.]{2,40})(?:\n|$|,)',
            
            # "@ Microsoft"
            r'@\s*([A-Z][A-Za-z0-9\s&\.]{2,40})(?:\n|$)',
            
            # "Join Adobe as"
            r'Join\s+([A-Z][A-Za-z0-9\s&\.]{2,40})\s+as',
            
            # First line if it's a company name
            r'^([A-Z][A-Za-z0-9\s&\.]{3,30})(?:\n|$)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.MULTILINE | re.IGNORECASE)
            if match:
                company = match.group(1).strip()
                # Filter out common false positives
                if company and not re.match(r'^(Role|Position|Location|Experience|Salary|Apply|Join|Hiring)', company, re.IGNORECASE):
                    # Clean up
                    company = re.sub(r'\s+', ' ', company)
                    company = company.strip('.,!?')
                    return company[:100]  # Max 100 chars
        
        return None
    
    def _extract_job_title(self, text: str, company_name: Optional[str] = None) -> Optional[str]:
        """Extract job title with context awareness"""
        patterns = [
            # "Role: Software Engineer"
            r'(?:Role|Position|Opening|Title)\s*:?\s*([A-Z][A-Za-z\s\(\)]+(?:Engineer|Developer|Manager|Analyst|Designer|Architect|Lead|Intern|Specialist|Consultant|Executive|Coordinator))',
            
            # "hiring for Backend Developer"
            r'hiring\s+for\s+([A-Z][A-Za-z\s\(\)]+)',
            
            # "Position: Senior SDE"
            r'Position\s*:?\s*([A-Z][A-Za-z\s\(\)]+)',
            
            # Common job titles at start of line
            r'^([A-Z][A-Za-z\s\(\)]+(?:Engineer|Developer|Manager|Analyst|Designer|Architect|Lead|Intern|Specialist))',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.MULTILINE | re.IGNORECASE)
            if match:
                title = match.group(1).strip()
                # Clean up
                title = re.sub(r'\s+', ' ', title)
                title = title.strip('.,!?:')
                
                # Don't return if it's the company name
                if company_name and title.lower() == company_name.lower():
                    continue
                
                if len(title) > 5 and len(title) < 100:
                    return title
        
        # Fallback: Look for job keywords in first 100 chars
        job_keywords = ['Engineer', 'Developer', 'Manager', 'Analyst', 'Designer', 'Architect', 'Intern']
        first_line = text.split('\n')[0]
        for keyword in job_keywords:
            if keyword.lower() in first_line.lower():
                words = first_line.split()
                for i, word in enumerate(words):
                    if keyword.lower() in word.lower():
                        # Get 2-3 words around the keyword
                        start = max(0, i-2)
                        end = min(len(words), i+2)
                        title = ' '.join(words[start:end])
                        if len(title) > 5:
                            return title[:100]
        
        return None
    
    def _extract_location(self, text: str) -> Optional[str]:
        """Extract job location (legacy method - use _extract_location_enhanced instead)"""
        location_data = self._extract_location_enhanced(text)
        return location_data.get('raw_location')
    
    def _extract_location_enhanced(self, text: str) -> Dict:
        """
        Extract location with enhanced intelligence
        
        Returns structured data:
        {
            'raw_location': 'Bangalore / Remote',
            'cities': ['bangalore'],
            'is_remote': True,
            'is_hybrid': False,
            'is_onsite_only': False,
            'geographic_scope': 'india' | 'international' | 'unspecified'
        }
        """
        result = {
            'raw_location': None,
            'cities': [],
            'is_remote': False,
            'is_hybrid': False,
            'is_onsite_only': False,
            'geographic_scope': 'unspecified'
        }
        
        text_lower = text.lower()
        
        # Extract raw location string
        patterns = [
            # "Location: Bangalore"
            r'Location\s*:?\s*([A-Za-z\s,\(\)]+?)(?:\n|$|Experience|Salary|Apply)',
            # "Bangalore, India"
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?),?\s*(?:India|Hybrid|Remote)',
            # "Remote" or "WFH"
            r'\b(Remote|Work\s+from\s+home|WFH)\b',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                location = match.group(1).strip()
                location = re.sub(r'\s+', ' ', location)
                location = location.strip('.,!?:')
                if len(location) > 2 and len(location) < 100:
                    result['raw_location'] = location
                    break
        
        # Analyze work mode
        # First check for negative patterns (no remote, not remote, etc.)
        negative_remote_patterns = [
            'no remote', 'not remote', 'no wfh', 'not wfh',
            'no work from home', 'cannot work from home'
        ]
        has_negative_remote = any(p in text_lower for p in negative_remote_patterns)
        
        if not has_negative_remote:
            remote_keywords = ['remote', 'wfh', 'work from home', 'work from anywhere']
            if any(kw in text_lower for kw in remote_keywords):
                result['is_remote'] = True
        
        hybrid_keywords = ['hybrid', 'flexible work']
        if any(kw in text_lower for kw in hybrid_keywords):
            result['is_hybrid'] = True
        
        # Check for onsite-only indicators
        for keyword in self.ONSITE_ONLY_KEYWORDS:
            if keyword in text_lower:
                result['is_onsite_only'] = True
                break
        
        # Identify cities mentioned
        for city in self.INDIA_CITIES:
            if city in text_lower:
                result['cities'].append(city)
        
        # Determine geographic scope
        has_india_location = len(result['cities']) > 0 or 'india' in text_lower
        has_international = any(kw in text_lower for kw in self.INTERNATIONAL_KEYWORDS)
        
        if has_international and not has_india_location:
            result['geographic_scope'] = 'international'
        elif has_india_location:
            result['geographic_scope'] = 'india'
        else:
            result['geographic_scope'] = 'unspecified'
        
        # Override onsite_only if remote/hybrid is mentioned
        if result['is_remote'] or result['is_hybrid']:
            result['is_onsite_only'] = False
        
        return result
    
    def _extract_salary(self, text: str) -> Optional[Tuple[int, int, str]]:
        """
        Extract salary information
        
        Returns:
            (min_salary, max_salary, raw_string) or None
        """
        patterns = [
            # "Salary: 5-7 LPA"
            r'(?:Salary|CTC|Package|Compensation)\s*:?\s*(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*(?:LPA|lakh|lakhs?)',
            
            # "12-24 LPA"
            r'(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*(?:LPA|lakh|lakhs?)',
            
            # "Salary: 5 LPA"
            r'(?:Salary|CTC|Package)\s*:?\s*(\d+(?:\.\d+)?)\s*(?:LPA|lakh|lakhs?)',
            
            # "₹5,00,000 - ₹8,00,000"
            r'₹\s*(\d+)(?:,\d+)*\s*-\s*₹?\s*(\d+)(?:,\d+)*',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                raw = match.group(0)
                
                # Parse numbers
                if 'LPA' in raw.upper() or 'lakh' in raw.lower():
                    # LPA format
                    nums = re.findall(r'(\d+(?:\.\d+)?)', raw)
                    if len(nums) >= 2:
                        min_sal = int(float(nums[0]) * 100000)
                        max_sal = int(float(nums[1]) * 100000)
                        return (min_sal, max_sal, raw)
                    elif len(nums) == 1:
                        sal = int(float(nums[0]) * 100000)
                        return (sal, sal, raw)
                else:
                    # Rupee format
                    nums = re.findall(r'(\d+)', raw.replace(',', ''))
                    if len(nums) >= 2:
                        return (int(nums[0]), int(nums[1]), raw)
        
        return None
    
    def _extract_experience(self, text: str) -> Optional[Tuple[int, int, str, bool]]:
        """
        Extract experience requirement
        
        Returns:
            (min_years, max_years, raw_string, is_fresher_friendly) or None
        """
        # Check for fresher keywords first
        fresher_patterns = [
            r'\b(fresher|freshers?|entry\s+level|0\s+years?)\b',
        ]
        
        is_fresher = False
        for pattern in fresher_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                is_fresher = True
                break
        
        # Extract experience range
        patterns = [
            # "Experience: 2-5 years"
            r'Experience\s*:?\s*(\d+)\s*-\s*(\d+)\s*(?:years?|yrs?)',
            
            # "2-5 years experience"
            r'(\d+)\s*-\s*(\d+)\s*(?:years?|yrs?)',
            
            # "Experience: 3+ years"
            r'Experience\s*:?\s*(\d+)\+?\s*(?:years?|yrs?)',
            
            # "3+ years"
            r'(\d+)\+\s*(?:years?|yrs?)',
            
            # "Minimum 2 years"
            r'(?:Minimum|Min|Atleast)\s*(\d+)\s*(?:years?|yrs?)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                raw = match.group(0)
                nums = re.findall(r'(\d+)', raw)
                
                if len(nums) >= 2:
                    return (int(nums[0]), int(nums[1]), raw, is_fresher)
                elif len(nums) == 1:
                    exp = int(nums[0])
                    return (exp, exp + 2, raw, is_fresher)  # Assume +2 year range
        
        # If fresher mentioned but no number
        if is_fresher:
            return (0, 1, "Fresher", True)
        
        return None
    
    def _extract_emails(self, text: str) -> List[str]:
        """Extract all email addresses"""
        pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails = re.findall(pattern, text)
        return list(set(emails))[:5]  # Max 5 unique emails
    
    def _extract_phones(self, text: str) -> List[str]:
        """Extract Indian phone numbers"""
        patterns = [
            r'\+91[-\s]?\d{10}',  # +91-9876543210
            r'\d{10}',  # 9876543210
            r'\d{3}[-\s]\d{3}[-\s]\d{4}',  # 987-654-3210
        ]
        
        phones = []
        for pattern in patterns:
            matches = re.findall(pattern, text)
            phones.extend(matches)
        
        return list(set(phones))[:3]  # Max 3 unique phones
    
    def _extract_apply_link(self, text: str, links: List[str] = None) -> Optional[str]:
        """Extract application link"""
        # First try to find link after "Apply" keyword
        apply_pattern = r'(?:Apply|Click|Visit).*?(https?://[^\s]+)'
        match = re.search(apply_pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        
        # Otherwise, return first career/job link
        if links:
            for link in links:
                if any(keyword in link.lower() for keyword in ['career', 'job', 'apply', 'ashby', 'lever', 'greenhouse', 'workday']):
                    return link
            # Return first link if no career link found
            if links:
                return links[0]
        
        # Extract any URL from text
        url_pattern = r'https?://[^\s]+'
        match = re.search(url_pattern, text)
        if match:
            return match.group(0)
        
        return None
    
    def _extract_skills(self, text: str) -> List[str]:
        """Extract technical skills"""
        # Common tech skills
        skill_keywords = [
            'Python', 'Java', 'JavaScript', 'TypeScript', 'React', 'Node', 'Angular', 'Vue',
            'Django', 'Flask', 'FastAPI', 'Spring', 'Express',
            'SQL', 'PostgreSQL', 'MySQL', 'MongoDB', 'Redis',
            'AWS', 'Azure', 'GCP', 'Docker', 'Kubernetes',
            'Machine Learning', 'AI', 'Data Science', 'ML', 'Deep Learning',
            'DevOps', 'CI/CD', 'Git', 'Linux',
            'Android', 'iOS', 'Flutter', 'React Native',
        ]
        
        found_skills = []
        text_lower = text.lower()
        
        for skill in skill_keywords:
            if skill.lower() in text_lower:
                found_skills.append(skill)
        
        return list(set(found_skills))[:10]  # Max 10 skills
    
    def _calculate_extraction_confidence(self, extraction: EnhancedJobExtraction) -> float:
        """Calculate confidence score based on extracted fields"""
        score = 0.0
        
        # Required fields
        if extraction.company_name:
            score += 0.3
        if extraction.job_title:
            score += 0.3
        
        # Important fields
        if extraction.location:
            score += 0.1
        if extraction.salary_raw:
            score += 0.1
        if extraction.apply_link:
            score += 0.1
        
        # Nice to have
        if extraction.experience_raw:
            score += 0.05
        if extraction.contact_emails:
            score += 0.05
        
        return min(score, 1.0)
    
    def get_or_create_company(self, db: Session, company_name: str) -> Optional[Company]:
        """
        Get existing company or create new one
        
        Uses in-memory cache to avoid repeated DB queries
        """
        if not company_name:
            return None
        
        # Normalize company name
        normalized = self._normalize_company_name(company_name)
        
        # Check cache
        if normalized in self.company_cache:
            return self.company_cache[normalized]
        
        # Check database (case-insensitive)
        company = db.query(Company).filter(
            Company.name.ilike(normalized)
        ).first()
        
        if company:
            self.company_cache[normalized] = company
            return company
        
        # Create new company
        company = Company(
            name=normalized,
            is_verified="unverified"
        )
        db.add(company)
        db.flush()  # Get ID without committing
        
        self.company_cache[normalized] = company
        return company
    
    def _normalize_company_name(self, name: str) -> str:
        """Normalize company name for matching"""
        # Remove extra spaces
        name = re.sub(r'\s+', ' ', name)
        
        # Remove common suffixes
        name = re.sub(r'\s+(Inc|LLC|Ltd|Limited|Corp|Corporation|Pvt|Private)\.?$', '', name, flags=re.IGNORECASE)
        
        # Trim
        name = name.strip('.,!?')
        
        # Title case
        name = name.title()
        
        return name[:100]


# Global instance
_extractor = None

def get_enhanced_extractor() -> EnhancedJobExtractor:
    """Get global enhanced extractor instance"""
    global _extractor
    if _extractor is None:
        _extractor = EnhancedJobExtractor()
    return _extractor
