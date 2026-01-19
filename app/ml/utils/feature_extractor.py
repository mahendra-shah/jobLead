"""
Feature extraction for ML job classification
Extracts keywords, patterns, and statistical features
"""

import re
from typing import Dict, List
from app.ml.utils.text_preprocessor import text_preprocessor


class FeatureExtractor:
    """Extract features from text for ML classification"""
    
    # Job-related keywords (strong indicators)
    JOB_KEYWORDS = {
        'hiring', 'job', 'position', 'role', 'opening', 'opportunity', 'career',
        'vacancy', 'recruitment', 'apply', 'candidate', 'join', 'team',
        'looking for', 'seeking', 'required', 'experience', 'skills',
        'salary', 'ctc', 'package', 'compensation', 'benefits',
        'interview', 'resume', 'cv', 'application', 'requirements',
        'responsibilities', 'qualifications', 'eligibility'
    }
    
    # Job titles (common roles)
    JOB_TITLES = {
        'developer', 'engineer', 'programmer', 'analyst', 'manager',
        'consultant', 'designer', 'architect', 'lead', 'senior',
        'junior', 'intern', 'trainee', 'associate', 'specialist',
        'coordinator', 'executive', 'officer', 'director', 'head',
        'data scientist', 'software engineer', 'web developer',
        'full stack', 'backend', 'frontend', 'devops', 'qa',
        'tester', 'product manager', 'business analyst', 'hr'
    }
    
    # Technical skills
    TECH_SKILLS = {
        'python', 'java', 'javascript', 'react', 'angular', 'vue',
        'node', 'django', 'flask', 'spring', 'aws', 'azure', 'gcp',
        'docker', 'kubernetes', 'sql', 'mongodb', 'postgresql',
        'machine learning', 'ml', 'ai', 'data science', 'analytics',
        'tensorflow', 'pytorch', 'scikit-learn', 'pandas', 'numpy',
        'rest api', 'microservices', 'agile', 'scrum', 'git',
        'linux', 'shell scripting', 'ci/cd', 'jenkins', 'testing'
    }
    
    # Work locations
    LOCATIONS = {
        'bangalore', 'bengaluru', 'mumbai', 'delhi', 'hyderabad',
        'pune', 'chennai', 'kolkata', 'gurgaon', 'noida',
        'remote', 'work from home', 'wfh', 'hybrid', 'onsite',
        'india', 'usa', 'uk', 'singapore', 'dubai'
    }
    
    # Job types
    JOB_TYPES = {
        'full time', 'full-time', 'fulltime', 'part time', 'part-time',
        'contract', 'freelance', 'internship', 'permanent', 'temporary'
    }
    
    # Company indicators
    COMPANY_KEYWORDS = {
        'company', 'organization', 'firm', 'startup', 'corporation',
        'pvt ltd', 'private limited', 'inc', 'llc', 'technologies',
        'solutions', 'services', 'systems', 'software', 'consulting'
    }
    
    # Non-job indicators (help filter out false positives)
    NON_JOB_KEYWORDS = {
        'meme', 'joke', 'funny', 'lol', 'lmao', 'haha',
        'breaking news', 'update', 'announcement', 'event',
        'webinar', 'workshop', 'course', 'training', 'certification',
        'exam', 'result', 'admit card', 'syllabus',
        'birthday', 'congratulations', 'wishes', 'greetings',
        'sale', 'discount', 'offer', 'deal', 'price'
    }
    
    # Experience patterns
    EXPERIENCE_PATTERNS = [
        r'\b(\d+)\+?\s*(?:years?|yrs?)\b',
        r'\b(\d+)\s*-\s*(\d+)\s*(?:years?|yrs?)\b',
        r'\bfresher\b',
        r'\bentry\s*level\b'
    ]
    
    # Salary patterns
    SALARY_PATTERNS = [
        r'\b(\d+)\s*(?:lpa|lakh|lakhs)\b',
        r'\b(\d+)\s*-\s*(\d+)\s*(?:lpa|lakh|lakhs)\b',
        r'\bctc\s*:\s*(\d+)',
        r'\bsalary\s*:\s*(\d+)',
        r'\₹\s*(\d+(?:,\d+)*)'
    ]
    
    def __init__(self):
        pass
    
    def extract_keyword_features(self, text: str) -> Dict[str, any]:
        """
        Extract keyword-based features
        
        Args:
            text: Preprocessed text
            
        Returns:
            Dictionary of keyword features
        """
        text_lower = text.lower()
        
        # Count matches
        job_keyword_count = sum(1 for kw in self.JOB_KEYWORDS if kw in text_lower)
        job_title_count = sum(1 for title in self.JOB_TITLES if title in text_lower)
        tech_skill_count = sum(1 for skill in self.TECH_SKILLS if skill in text_lower)
        location_count = sum(1 for loc in self.LOCATIONS if loc in text_lower)
        job_type_count = sum(1 for jt in self.JOB_TYPES if jt in text_lower)
        company_keyword_count = sum(1 for kw in self.COMPANY_KEYWORDS if kw in text_lower)
        non_job_keyword_count = sum(1 for kw in self.NON_JOB_KEYWORDS if kw in text_lower)
        
        return {
            'has_job_keywords': job_keyword_count > 0,
            'job_keyword_count': job_keyword_count,
            'has_job_title': job_title_count > 0,
            'job_title_count': job_title_count,
            'has_tech_skills': tech_skill_count > 0,
            'tech_skill_count': tech_skill_count,
            'has_location': location_count > 0,
            'location_count': location_count,
            'has_job_type': job_type_count > 0,
            'job_type_count': job_type_count,
            'has_company_keywords': company_keyword_count > 0,
            'company_keyword_count': company_keyword_count,
            'has_non_job_keywords': non_job_keyword_count > 0,
            'non_job_keyword_count': non_job_keyword_count,
        }
    
    def extract_pattern_features(self, text: str) -> Dict[str, any]:
        """
        Extract pattern-based features (experience, salary, etc.)
        
        Args:
            text: Preprocessed text
            
        Returns:
            Dictionary of pattern features
        """
        text_lower = text.lower()
        
        # Experience patterns
        has_experience = any(re.search(pattern, text_lower) for pattern in self.EXPERIENCE_PATTERNS)
        
        # Salary patterns
        has_salary = any(re.search(pattern, text_lower) for pattern in self.SALARY_PATTERNS)
        
        # Contact info patterns
        has_email = bool(text_preprocessor.extract_emails(text))
        has_phone = bool(text_preprocessor.extract_phone_numbers(text))
        has_url = bool(text_preprocessor.extract_urls(text))
        
        # Application indicators
        has_apply_link = bool(re.search(r'\bapply\s+(?:here|now|link)\b', text_lower))
        has_email_resume = bool(re.search(r'\b(?:send|mail|email)\s+(?:resume|cv)\b', text_lower))
        
        return {
            'has_experience_requirement': has_experience,
            'has_salary_info': has_salary,
            'has_email': has_email,
            'has_phone': has_phone,
            'has_url': has_url,
            'has_apply_link': has_apply_link,
            'has_email_resume': has_email_resume,
            'has_contact_info': has_email or has_phone,
            'has_application_method': has_apply_link or has_email_resume or has_email,
        }
    
    def extract_statistical_features(self, text: str) -> Dict[str, any]:
        """
        Extract statistical features from text
        
        Args:
            text: Preprocessed text
            
        Returns:
            Dictionary of statistical features
        """
        stats = text_preprocessor.get_text_stats(text)
        
        return {
            'char_count': stats['char_count'],
            'word_count': stats['word_count'],
            'sentence_count': stats['sentence_count'],
            'avg_word_length': stats['avg_word_length'],
            'has_minimum_length': stats['word_count'] >= 20,  # Jobs usually >20 words
            'has_maximum_length': stats['word_count'] <= 500,  # Jobs usually <500 words
            'is_reasonable_length': 20 <= stats['word_count'] <= 500,
            'url_count': stats['url_count'],
            'email_count': stats['email_count'],
            'phone_count': stats['phone_count'],
            'has_multiple_sentences': stats['sentence_count'] > 1,
        }
    
    def extract_structure_features(self, text: str) -> Dict[str, any]:
        """
        Extract structural features (formatting, organization)
        
        Args:
            text: Preprocessed text
            
        Returns:
            Dictionary of structural features
        """
        # Check for structured content
        has_bullet_points = bool(re.search(r'[•\-*]\s+', text))
        has_numbered_list = bool(re.search(r'\d+\.\s+', text))
        has_sections = bool(re.search(r'(?:responsibilities|requirements|qualifications|skills|about):', text.lower()))
        has_line_breaks = '\n' in text
        
        # Count capitalized words (company names, job titles)
        words = text.split()
        capitalized_count = sum(1 for w in words if w and w[0].isupper())
        capitalization_ratio = capitalized_count / len(words) if words else 0
        
        return {
            'has_bullet_points': has_bullet_points,
            'has_numbered_list': has_numbered_list,
            'has_sections': has_sections,
            'has_line_breaks': has_line_breaks,
            'is_structured': has_bullet_points or has_numbered_list or has_sections,
            'capitalization_ratio': capitalization_ratio,
            'has_high_capitalization': capitalization_ratio > 0.2,
        }
    
    def extract_all(self, text: str) -> Dict[str, any]:
        """
        Extract all features from text
        
        Args:
            text: Raw text to extract features from
            
        Returns:
            Dictionary with all features
        """
        # Preprocess text
        preprocessed = text_preprocessor.preprocess_for_ml(text)
        
        # Extract all feature types
        features = {}
        features.update(self.extract_keyword_features(preprocessed))
        features.update(self.extract_pattern_features(text))  # Use original for patterns
        features.update(self.extract_statistical_features(preprocessed))
        features.update(self.extract_structure_features(text))  # Use original for structure
        
        # Add composite features
        features['job_signal_strength'] = (
            features['job_keyword_count'] +
            features['job_title_count'] +
            features['tech_skill_count'] * 0.5
        )
        
        features['completeness_score'] = sum([
            features['has_job_title'],
            features['has_location'],
            features['has_tech_skills'],
            features['has_contact_info'],
            features['has_application_method'],
            features['is_structured'],
        ]) / 6.0
        
        features['is_likely_job'] = (
            features['has_job_keywords'] and
            features['has_job_title'] and
            features['is_reasonable_length'] and
            not features['has_non_job_keywords']
        )
        
        return features
    
    def get_feature_names(self) -> List[str]:
        """
        Get list of all feature names
        Used for ML model training
        
        Returns:
            List of feature names
        """
        # Extract features from a sample text to get all feature names
        sample_features = self.extract_all("Sample job posting text")
        return list(sample_features.keys())
    
    def features_to_vector(self, features: Dict[str, any]) -> List[float]:
        """
        Convert feature dictionary to numeric vector
        
        Args:
            features: Feature dictionary
            
        Returns:
            List of numeric values
        """
        vector = []
        for key in sorted(features.keys()):
            value = features[key]
            if isinstance(value, bool):
                vector.append(1.0 if value else 0.0)
            elif isinstance(value, (int, float)):
                vector.append(float(value))
            else:
                vector.append(0.0)  # Default for unknown types
        return vector


# Global instance
feature_extractor = FeatureExtractor()
