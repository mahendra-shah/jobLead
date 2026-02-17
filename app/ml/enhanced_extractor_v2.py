"""
Enhanced Job Extractor V2 - Improved Accuracy

Key improvements over V1:
1. Better company name extraction (@mentions, special chars, common prefixes)
2. Enhanced salary parsing (LPA, USD, lakhs, monthly/annual conversion)
3. Location normalization (standardize city names)
4. Improved skills extraction with tech stack patterns
5. Better handling of Indian job market formats
"""

import re
from typing import List, Dict, Optional, Tuple, Set
from dataclasses import dataclass
import structlog

logger = structlog.get_logger(__name__)


# Location mapping for standardization
LOCATION_MAPPINGS = {
    'bengaluru': 'Bangalore',
    'bangalore': 'Bangalore',
    'blr': 'Bangalore',
    'mumbai': 'Mumbai',
    'bombay': 'Mumbai',
    'bom': 'Mumbai',
    'delhi': 'Delhi',
    'new delhi': 'Delhi',
    'ncr': 'Delhi NCR',
    'gurgaon': 'Gurugram',
    'gurugram': 'Gurugram',
    'noida': 'Noida',
    'hyderabad': 'Hyderabad',
    'hyd': 'Hyderabad',
    'chennai': 'Chennai',
    'madras': 'Chennai',
    'pune': 'Pune',
    'kolkata': 'Kolkata',
    'calcutta': 'Kolkata',
    'remote': 'Remote',
    'wfh': 'Remote',
    'work from home': 'Remote',
}

# Common tech skills for extraction
TECH_SKILLS = [
    # Programming Languages
    'python', 'java', 'javascript', 'typescript', 'c\\+\\+', 'c#', 'ruby', 'php', 
    'golang', 'go', 'rust', 'kotlin', 'swift', 'scala', 'r', 'perl',
    
    # Frontend
    'react', 'angular', 'vue', 'nextjs', 'nuxt', 'svelte', 'html', 'css', 'sass',
    'tailwind', 'bootstrap', 'webpack', 'vite',
    
    # Backend
    'node', 'nodejs', 'express', 'django', 'flask', 'fastapi', 'spring', 'springboot',
    'rails', 'laravel', 'asp\\.net', 'nest', 'nestjs',
    
    # Databases
    'mysql', 'postgresql', 'postgres', 'mongodb', 'redis', 'elasticsearch', 'dynamodb',
    'cassandra', 'oracle', 'mssql', 'sqlite', 'mariadb',
    
    # Cloud & DevOps
    'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'k8s', 'jenkins', 'gitlab',
    'github actions', 'terraform', 'ansible', 'ci/cd', 'linux',
    
    # Data Science & ML
    'machine learning', 'deep learning', 'tensorflow', 'pytorch', 'keras', 'scikit-learn',
    'pandas', 'numpy', 'spark', 'hadoop', 'airflow', 'tableau', 'power bi',
    
    # Mobile
    'android', 'ios', 'react native', 'flutter', 'xamarin',
    
    # Other
    'graphql', 'rest api', 'microservices', 'agile', 'scrum', 'jira', 'git',
]


@dataclass
class EnhancedJobExtractionV2:
    """Enhanced job extraction with improved accuracy"""
    # Basic info
    job_title: Optional[str] = None
    company_name: Optional[str] = None
    company_id: Optional[str] = None
    
    # Job details
    location: Optional[str] = None
    location_normalized: Optional[str] = None  # Standardized location
    salary_min: Optional[int] = None
    salary_max: Optional[int] = None
    salary_currency: str = "INR"
    salary_raw: Optional[str] = None
    salary_period: str = "annual"  # annual or monthly
    
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
    skills_extracted_count: int = 0
    description: str = ""
    
    # Metadata
    confidence: float = 0.0
    raw_text: str = ""
    extraction_version: str = "v2"
    
    def __post_init__(self):
        if self.contact_emails is None:
            self.contact_emails = []
        if self.contact_phones is None:
            self.contact_phones = []
        if self.skills is None:
            self.skills = []


class EnhancedJobExtractorV2:
    """
    V2 Extractor with significant improvements:
    - Better regex patterns for Indian job market
    - Salary normalization (convert all to monthly INR)
    - Location standardization
    - Enhanced company name extraction
    """
    
    def __init__(self):
        self.tech_skills_pattern = self._compile_skills_pattern()
        logger.info("extractor_v2_initialized", skills_count=len(TECH_SKILLS))
    
    def _compile_skills_pattern(self) -> re.Pattern:
        """Compile regex pattern for all tech skills"""
        # Sort by length descending to match longer phrases first
        sorted_skills = sorted(TECH_SKILLS, key=len, reverse=True)
        pattern = '|'.join([re.escape(skill) for skill in sorted_skills])
        return re.compile(f'\\b({pattern})\\b', re.IGNORECASE)
    
    def extract_jobs_from_message(self, text: str, links: List[str] = None) -> List[EnhancedJobExtractionV2]:
        """
        Main entry point: Extract one or more jobs from a message
        
        Returns:
            List of EnhancedJobExtractionV2 (one per job found)
        """
        logger.debug("extracting_jobs_v2", text_length=len(text))
        
        # Step 1: Split multiple jobs if present
        job_sections = self._split_into_jobs(text)
        
        # Step 2: Extract from each section
        extractions = []
        for section in job_sections:
            extraction = self._extract_single_job(section, links)
            if extraction and extraction.confidence > 0.3:
                extractions.append(extraction)
        
        logger.info("jobs_extracted_v2", count=len(extractions))
        return extractions
    
    def _split_into_jobs(self, text: str) -> List[str]:
        """Split message into individual job sections"""
        # Pattern 1: Numbered lists (1. Python Dev\n2. React Dev)
        numbered_pattern = r'(?:^|\n)(\d+[\.\)]\s+.+?)(?=\n\d+[\.\)]|\Z)'
        numbered_matches = re.findall(numbered_pattern, text, re.DOTALL | re.MULTILINE)
        
        if len(numbered_matches) >= 2:
            return [match.strip() for match in numbered_matches]
        
        # Pattern 2: Multiple "Company is hiring" blocks
        company_hiring_pattern = r'(?:^|\n)([A-Z][A-Za-z0-9\s&\.]{2,40}?\s+(?:is\s+)?hiring.+?)(?=[A-Z][A-Za-z0-9\s&\.]+?\s+(?:is\s+)?hiring|\Z)'
        company_blocks = re.findall(company_hiring_pattern, text, re.DOTALL | re.IGNORECASE)
        
        if len(company_blocks) >= 2:
            return [block.strip() for block in company_blocks]
        
        # No multiple jobs detected
        return [text]
    
    def _extract_single_job(self, text: str, links: List[str] = None) -> Optional[EnhancedJobExtractionV2]:
        """Extract all information from a single job posting"""
        extraction = EnhancedJobExtractionV2()
        extraction.raw_text = text
        extraction.description = text
        
        # Extract each component
        extraction.company_name = self._extract_company_v2(text)
        extraction.job_title = self._extract_job_title_v2(text, extraction.company_name)
        
        # Location with normalization
        location_raw = self._extract_location_v2(text)
        extraction.location = location_raw
        extraction.location_normalized = self._normalize_location(location_raw) if location_raw else None
        
        # Salary with better parsing
        salary_info = self._extract_salary_v2(text)
        if salary_info:
            extraction.salary_min, extraction.salary_max, extraction.salary_raw, extraction.salary_period = salary_info
            # Normalize to monthly if annual
            if extraction.salary_period == "annual":
                extraction.salary_min = extraction.salary_min // 12 if extraction.salary_min else None
                extraction.salary_max = extraction.salary_max // 12 if extraction.salary_max else None
                extraction.salary_period = "monthly"
        
        # Experience
        exp_info = self._extract_experience_v2(text)
        if exp_info:
            extraction.experience_min, extraction.experience_max, extraction.experience_raw, extraction.is_fresher_friendly = exp_info
        
        # Contact info
        extraction.contact_emails = self._extract_emails(text)
        extraction.apply_email = extraction.contact_emails[0] if extraction.contact_emails else None
        extraction.contact_phones = self._extract_phones(text)
        
        # Apply link
        extraction.apply_link = self._extract_apply_link(text, links)
        
        # Skills with improved extraction
        extraction.skills = self._extract_skills_v2(text)
        extraction.skills_extracted_count = len(extraction.skills)
        
        # Calculate confidence
        extraction.confidence = self._calculate_extraction_confidence_v2(extraction)
        
        logger.debug("extraction_v2_complete", 
                    company=extraction.company_name,
                    title=extraction.job_title,
                    confidence=extraction.confidence)
        
        return extraction
    
    def _extract_company_v2(self, text: str) -> Optional[str]:
        """
        Enhanced company name extraction with better patterns
        
        Improvements:
        - Handle @mentions (e.g., @Google, @TechCorp)
        - Better special character handling
        - Filter common false positives
        - Clean up prefixes/suffixes
        """
        patterns = [
            # Pattern 1: "@Company" mentions (common in Telegram)
            r'@([A-Z][A-Za-z0-9_]{2,30})',
            
            # Pattern 2: "Company is hiring"
            r'(?:^|\n)([A-Z][A-Za-z0-9\s&\.\'-]{2,40}?)\s+is\s+hiring',
            
            # Pattern 3: "Company: Google" or "Hiring for: Adobe"
            r'(?:Company|Organization|Hiring\s+for|Client)\s*:?\s*([A-Z][A-Za-z0-9\s&\.\'-]{2,40})(?:\n|$|,)',
            
            # Pattern 4: "Join Google as"
            r'Join\s+([A-Z][A-Z a-z0-9\s&\.\'-]{2,40})\s+as',
            
            # Pattern 5: Quoted company name "Google"
            r'["\']([A-Z][A-Za-z0-9\s&\.]{2,30})["\']',
            
            # Pattern 6: First capitalized phrase (fallback)
            r'(?:^|\n)([A-Z][A-Za-z0-9\s&\.]{3,30})(?:\n)',
        ]
        
        company_candidates = []
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.MULTILINE)
            for match in matches:
                company = match.strip()
                
                # Clean up
                company = self._clean_company_name(company)
                
                # Validate
                if self._is_valid_company_name(company):
                    company_candidates.append((company, patterns.index(pattern)))  # Store with priority
        
        # Return highest priority match
        if company_candidates:
            company_candidates.sort(key=lambda x: x[1])  # Sort by pattern priority
            return company_candidates[0][0]
        
        return None
    
    def _clean_company_name(self, company: str) -> str:
        """Clean and normalize company name"""
        # Remove common prefixes
        prefixes = [
            r'^(?:at|for|by|from)\s+',
            r'^(?:hiring\s+for)\s+',
        ]
        for prefix in prefixes:
            company = re.sub(prefix, '', company, flags=re.IGNORECASE)
        
        # Remove trailing punctuation and suffixes
        company = company.strip('.,!?:-—')
        company = re.sub(r'\s*\([^)]*\)$', '', company)  # Remove trailing (brackets)
        company = re.sub(r'\s+', ' ', company)  # Normalize spaces
        
        return company.strip()
    
    def _is_valid_company_name(self, company: str) -> bool:
        """Validate if string is likely a company name"""
        if not company or len(company) < 3 or len(company) > 50:
            return False
        
        # Filter common false positives
        false_positives = [
            'role', 'position', 'location', 'experience', 'salary', 'apply', 
            'join', 'hiring', 'urgent', 'immediate', 'required', 'looking',
            'job', 'opening', 'opportunity', 'career', 'work', 'company',
        ]
        
        if company.lower() in false_positives:
            return False
        
        # Must start with capital letter
        if not company[0].isupper():
            return False
        
        return True
    
    def _extract_job_title_v2(self, text: str, company_name: Optional[str] = None) -> Optional[str]:
        """Enhanced job title extraction"""
        patterns = [
            # "Role: Software Engineer"
            r'(?:Role|Position|Opening|Title|Designation)\s*:?\s*([A-Z][A-Za-z\s\/\(\)-]+(?:Engineer|Developer|Manager|Analyst|Designer|Architect|Lead|Intern|Specialist|Consultant|Executive|Coordinator|SDE|QA|Tester))',
            
            # "hiring for Backend Developer"
            r'hiring\s+(?:for\s+)?([A-Z][A-Za-z\s\/\(\)-]+)',
            
            # "looking for Senior SDE"
            r'looking\s+for\s+(?:a\s+)?([A-Z][A-Za-z\s\/\(\)-]+)',
            
            # Common job titles at start of line
            r'(?:^|\n)([A-Z][A-Za-z\s\(\)/]+(?:Engineer|Developer|Manager|Analyst|Designer|Architect|Lead|Intern|Specialist|SDE))',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.MULTILINE | re.IGNORECASE)
            if match:
                title = match.group(1).strip()
                
                # Clean up
                title = re.sub(r'\s+', ' ', title)
                title = title.strip('.,!?:-')
                
                # Validate
                if self._is_valid_job_title(title, company_name):
                    return title[:100]
        
        return None
    
    def _is_valid_job_title(self, title: str, company_name: Optional[str]) -> bool:
        """Validate job title"""
        if not title or len(title) < 5 or len(title) > 100:
            return False
        
        # Don't return if it's the company name
        if company_name and title.lower() == company_name.lower():
            return False
        
        return True
    
    def _extract_location_v2(self, text: str) -> Optional[str]:
        """Enhanced location extraction"""
        patterns = [
            # "Location: Bangalore"
            r'Location\s*:?\s*([A-Za-z\s,\(\)/]+?)(?:\n|$|Experience|Salary|Apply|Skills|CTC)',
            
            # "Bangalore, India" or "Bangalore/Hybrid"
            r'(?:^|\n|•|\*|-)\s*(?:Location|Based\s+in)\s*:?\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\s*(?:,\s*India|/|$)',
            
            # City names followed by location indicators
            r'\b(Bangalore|Mumbai|Delhi|Hyderabad|Chennai|Pune|Kolkata|Gurugram|Noida)\s*(?:,?\s*(?:India|Hybrid|Remote|Office))?',
            
            # Remote/WFH anywhere in text
            r'\b(Remote|Work\s+from\s+home|WFH|Hybrid)\b',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                location = match.group(1).strip()
                location = re.sub(r'\s+', ' ', location)
                location = location.strip('.,!?:-')
                
                if len(location) >= 2 and len(location) <= 100:
                    return location
        
        return None
    
    def _normalize_location(self, location: str) -> str:
        """Normalize location to standard city names"""
        if not location:
            return None
        
        location_lower = location.lower().strip()
        
        # Check mappings
        for key, value in LOCATION_MAPPINGS.items():
            if key in location_lower:
                return value
        
        # Return original if no mapping found
        return location.title()
    
    def _extract_salary_v2(self, text: str) -> Optional[Tuple[int, int, str, str]]:
        """
        Enhanced salary extraction with better format support
        
        Handles:
        - LPA (Lakhs Per Annum): "5-7 LPA"
        - Lakhs: "5-7 lakhs"
        - Rupees: "₹5,00,000 - ₹8,00,000"
        - Monthly: "50k-80k per month"
        - USD: "$80k - $120k"
        
        Returns:
            (min_salary_inr, max_salary_inr, raw_string, period) or None
            All salaries normalized to INR
        """
        patterns = [
            # Pattern 1: "5-7 LPA" or "5-7 lakh"
            (r'(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*(?:LPA|lakh|lakhs?)\b', 'lpa'),
            
            # Pattern 2: "5 LPA" (single value)
            (r'(?:Salary|CTC|Package)\s*:?\s*(\d+(?:\.\d+)?)\s*(?:LPA|lakh|lakhs?)\b', 'lpa_single'),
            
            # Pattern 3: "₹5,00,000 - ₹8,00,000" or "5,00,000 - 8,00,000"
            (r'₹?\s*(\d+)(?:,\d+)*\s*-\s*₹?\s*(\d+)(?:,\d+)*', 'rupees'),
            
            # Pattern 4: "50k-80k per month" or "50-80k monthly"
            (r'(\d+)k?\s*-\s*(\d+)k?\s*(?:per\s+month|monthly|pm|/month)', 'monthly_k'),
            
            # Pattern 5: "$80k - $120k" (USD)
            (r'\$\s*(\d+)k?\s*-\s*\$?\s*(\d+)k?', 'usd'),
            
            # Pattern 6: "Upto 10 LPA" or "Maximum 8 LPA"
            (r'(?:upto|up\s+to|max|maximum)\s*(\d+(?:\.\d+)?)\s*(?:LPA|lakh)', 'lpa_max'),
        ]
        
        for pattern, salary_type in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                raw = match.group(0)
                
                try:
                    if salary_type == 'lpa':
                        min_lpa = float(match.group(1))
                        max_lpa = float(match.group(2))
                        min_sal = int(min_lpa * 100000)  # Convert to rupees
                        max_sal = int(max_lpa * 100000)
                        return (min_sal, max_sal, raw, 'annual')
                    
                    elif salary_type == 'lpa_single':
                        lpa = float(match.group(1))
                        sal = int(lpa * 100000)
                        return (sal, sal, raw, 'annual')
                    
                    elif salary_type == 'lpa_max':
                        max_lpa = float(match.group(1))
                        max_sal = int(max_lpa * 100000)
                        # Assume min is 60% of max
                        min_sal = int(max_sal * 0.6)
                        return (min_sal, max_sal, raw, 'annual')
                    
                    elif salary_type == 'rupees':
                        # Remove commas and parse
                        min_str = match.group(1).replace(',', '')
                        max_str = match.group(2).replace(',', '')
                        min_sal = int(min_str)
                        max_sal = int(max_str)
                        # If > 100k, assume annual, else monthly
                        period = 'annual' if max_sal > 100000 else 'monthly'
                        return (min_sal, max_sal, raw, period)
                    
                    elif salary_type == 'monthly_k':
                        min_k = int(match.group(1))
                        max_k = int(match.group(2))
                        min_sal = min_k * 1000
                        max_sal = max_k * 1000
                        return (min_sal, max_sal, raw, 'monthly')
                    
                    elif salary_type == 'usd':
                        # Convert USD to INR (approximate rate: 83)
                        min_usd = int(match.group(1)) * 1000 if 'k' in raw else int(match.group(1))
                        max_usd = int(match.group(2)) * 1000 if 'k' in raw else int(match.group(2))
                        min_sal = int(min_usd * 83)  # Convert to INR
                        max_sal = int(max_usd * 83)
                        return (min_sal, max_sal, raw, 'annual')
                
                except (ValueError, IndexError) as e:
                    logger.warning("salary_parse_error", error=str(e), raw=raw)
                    continue
        
        return None
    
    def _extract_experience_v2(self, text: str) -> Optional[Tuple[int, int, str, bool]]:
        """Enhanced experience extraction"""
        # Check for fresher keywords
        fresher_patterns = [
            r'\b(fresher|freshers?|entry\s+level|0\s+years?|no\s+experience)\b',
        ]
        
        is_fresher = any(re.search(p, text, re.IGNORECASE) for p in fresher_patterns)
        
        # Extract experience range
        patterns = [
            # "Experience: 2-5 years"
            (r'Experience\s*:?\s*(\d+)\s*-\s*(\d+)\s*(?:years?|yrs?)', 'range'),
            
            # "2-5 years"
            (r'(\d+)\s*-\s*(\d+)\s*(?:years?|yrs?)\s*(?:experience|exp)?', 'range'),
            
            # "3+ years"
            (r'(\d+)\+\s*(?:years?|yrs?)', 'plus'),
            
            # "Minimum 2 years"
            (r'(?:minimum|min|atleast|at\s+least)\s*(\d+)\s*(?:years?|yrs?)', 'min'),
        ]
        
        for pattern, exp_type in patterns:
            match = re.search(pattern, text, re.IGNORECASE):
                raw = match.group(0)
                
                if exp_type == 'range':
                    min_exp = int(match.group(1))
                    max_exp = int(match.group(2))
                    return (min_exp, max_exp, raw, is_fresher)
                
                elif exp_type == 'plus':
                    exp = int(match.group(1))
                    return (exp, exp + 3, raw, is_fresher)
                
                elif exp_type == 'min':
                    min_exp = int(match.group(1))
                    return (min_exp, min_exp + 2, raw, is_fresher)
        
        # If only fresher mentioned
        if is_fresher:
            return (0, 1, "Fresher", True)
        
        return None
    
    def _extract_skills_v2(self, text: str) -> List[str]:
        """
        Enhanced skills extraction using compiled pattern
        
        Improvements:
        - Pre-compiled regex for speed
        - Comprehensive tech stack coverage
        - Case-insensitive matching
        - Deduplication
        """
        # Find all skill matches
        matches = self.tech_skills_pattern.findall(text)
        
        # Deduplicate and normalize
        skills_set: Set[str] = set()
        for match in matches:
            skill = match.strip().lower()
            
            # Normalize common variations
            if skill in ['nodejs', 'node.js']:
                skill = 'node'
            elif skill in ['reactjs', 'react.js']:
                skill = 'react'
            elif skill in ['k8s']:
                skill = 'kubernetes'
            elif skill in ['postgres']:
                skill = 'postgresql'
            
            skills_set.add(skill.title())
        
        # Sort alphabetically
        skills = sorted(list(skills_set))
        
        logger.debug("skills_extracted_v2", count=len(skills), skills=skills[:10])
        return skills[:20]  # Max 20 skills
    
    def _extract_emails(self, text: str) -> List[str]:
        """Extract email addresses"""
        pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        emails = re.findall(pattern, text)
        return list(set(emails))[:5]
    
    def _extract_phones(self, text: str) -> List[str]:
        """Extract phone numbers (Indian format)"""
        patterns = [
            r'\+91[-\s]?\d{10}',
            r'(?:^|\s)(\d{10})(?:\s|$)',
        ]
        
        phones = []
        for pattern in patterns:
            matches = re.findall(pattern, text)
            phones.extend(matches)
        
        return list(set(phones))[:3]
    
    def _extract_apply_link(self, text: str, links: List[str] = None) -> Optional[str]:
        """Extract application link"""
        # Look for "Apply" keyword followed by URL
        apply_pattern = r'(?:Apply|Click|Visit|Link).*?(https?://[^\s]+)'
        match = re.search(apply_pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        
        # Check provided links for career/job keywords
        if links:
            for link in links:
                if any(kw in link.lower() for kw in ['career', 'job', 'apply', 'hiring', 'ashby', 'lever', 'greenhouse']):
                    return link
            return links[0] if links else None
        
        return None
    
    def _calculate_extraction_confidence_v2(self, extraction: EnhancedJobExtractionV2) -> float:
        """
        Calculate confidence score based on extracted fields
        
        Scoring:
        - Company name: 0.25
        - Job title: 0.25
        - Location: 0.10
        - Salary: 0.15
        - Experience: 0.10
        - Skills (1+ skills): 0.10
        - Apply link/email: 0.05
        """
        score = 0.0
        
        if extraction.company_name:
            score += 0.25
        
        if extraction.job_title:
            score += 0.25
        
        if extraction.location:
            score += 0.10
        
        if extraction.salary_min and extraction.salary_max:
            score += 0.15
        
        if extraction.experience_min is not None:
            score += 0.10
        
        if extraction.skills and len(extraction.skills) > 0:
            score += 0.10
        
        if extraction.apply_link or extraction.apply_email:
            score += 0.05
        
        return round(score, 2)
