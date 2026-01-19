"""
spaCy-based entity extraction for job postings
Enhances extraction with NER for companies, locations, skills
"""

from typing import Optional
import re

from app.ml.base_classifier import ExtractionResult


class SpacyExtractor:
    """
    Enhanced entity extraction using spaCy NER
    Lazy loading - only loads spaCy if available
    """
    
    def __init__(self):
        self.nlp = None
        self.is_loaded = False
        self._load_spacy()
    
    def _load_spacy(self):
        """Lazy load spaCy model"""
        try:
            import spacy
            self.nlp = spacy.load("en_core_web_sm")
            self.is_loaded = True
            print("✅ spaCy model loaded successfully")
        except ImportError:
            print("⚠️  spaCy not installed - using basic extraction only")
            print("   Install with: pip install spacy && python -m spacy download en_core_web_sm")
        except OSError:
            print("⚠️  spaCy model not found - using basic extraction only")
            print("   Download with: python -m spacy download en_core_web_sm")
    
    def enhance_extraction(self, basic_result: ExtractionResult) -> ExtractionResult:
        """
        Enhance basic extraction with spaCy NER
        
        Args:
            basic_result: Basic extraction from sklearn classifier
            
        Returns:
            Enhanced ExtractionResult with better entity recognition
        """
        if not self.is_loaded:
            return basic_result
        
        try:
            doc = self.nlp(basic_result.raw_text)
            
            # Extract entities
            companies = []
            locations = []
            job_titles = []
            
            for ent in doc.ents:
                if ent.label_ == "ORG":
                    companies.append(ent.text)
                elif ent.label_ == "GPE":  # Geopolitical entity
                    locations.append(ent.text)
                elif ent.label_ == "WORK_OF_ART":
                    # Sometimes job titles are tagged as WORK_OF_ART
                    job_titles.append(ent.text)
            
            # Update company if not found or low confidence
            if companies and (not basic_result.company or basic_result.confidence_scores.get('company', 0) < 0.6):
                basic_result.company = companies[0]
                basic_result.confidence_scores['company'] = 0.8
            
            # Update location if not found
            if locations and not basic_result.location:
                basic_result.location = locations[0]
                basic_result.confidence_scores['location'] = 0.85
            
            # Try to extract job title using patterns
            if not basic_result.job_title:
                job_title = self._extract_job_title(basic_result.raw_text, doc)
                if job_title:
                    basic_result.job_title = job_title
                    basic_result.confidence_scores['job_title'] = 0.75
            
            # Extract experience requirement
            experience = self._extract_experience(basic_result.raw_text)
            if experience:
                basic_result.experience_required = experience
            
            # Extract salary
            salary = self._extract_salary(basic_result.raw_text)
            if salary:
                basic_result.salary = salary
            
            return basic_result
            
        except Exception as e:
            print(f"⚠️  spaCy extraction error: {e}")
            return basic_result
    
    def _extract_job_title(self, text: str, doc) -> Optional[str]:
        """Extract job title using patterns and NER"""
        text_lower = text.lower()
        
        # Common job title patterns
        patterns = [
            r'(?:hiring|looking for|seeking|required?)\s+(?:a\s+)?([A-Z][a-zA-Z\s]+(?:Developer|Engineer|Manager|Analyst|Designer|Architect|Lead|Specialist|Consultant))',
            r'(?:position|role|opening):\s*([A-Z][a-zA-Z\s]+)',
            r'([A-Z][a-zA-Z\s]+(?:Developer|Engineer|Manager|Analyst|Designer|Architect|Lead|Specialist|Consultant))(?:\s+(?:position|role|opening))',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1).strip()
        
        # Look for job title keywords
        job_keywords = [
            'developer', 'engineer', 'manager', 'analyst', 'designer',
            'architect', 'lead', 'senior', 'junior', 'intern',
            'specialist', 'consultant', 'coordinator', 'executive'
        ]
        
        for token in doc:
            if token.text.lower() in job_keywords:
                # Get surrounding tokens
                start = max(0, token.i - 2)
                end = min(len(doc), token.i + 3)
                phrase = doc[start:end].text
                
                # Clean and return
                phrase = phrase.strip()
                if len(phrase) > 5 and len(phrase) < 50:
                    return phrase
        
        return None
    
    def _extract_experience(self, text: str) -> Optional[str]:
        """Extract experience requirement"""
        patterns = [
            r'(\d+\+?\s*(?:years?|yrs?))\s+(?:of\s+)?experience',
            r'(\d+\s*-\s*\d+\s*(?:years?|yrs?))\s+(?:of\s+)?experience',
            r'experience\s*:\s*(\d+\+?\s*(?:years?|yrs?))',
            r'(fresher|entry\s*level)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        return None
    
    def _extract_salary(self, text: str) -> Optional[str]:
        """Extract salary information"""
        patterns = [
            r'(\d+\s*-\s*\d+\s*(?:lpa|lakh|lakhs))',
            r'(\d+\s*(?:lpa|lakh|lakhs))',
            r'ctc\s*:\s*(\d+[\d,]*)',
            r'salary\s*:\s*(\d+[\d,]*)',
            r'(₹\s*\d+(?:,\d+)*(?:\s*-\s*₹?\s*\d+(?:,\d+)*)?)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1).strip()
        
        return None


# Global instance
_spacy_extractor = None

def get_spacy_extractor() -> SpacyExtractor:
    """Get global spaCy extractor instance"""
    global _spacy_extractor
    if _spacy_extractor is None:
        _spacy_extractor = SpacyExtractor()
    return _spacy_extractor
