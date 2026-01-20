"""
Job data parsing utilities for extracting structured information from text.

This module provides functions to parse experience and salary information
from job descriptions and convert them to structured database fields.
"""

import re
from typing import Dict, Optional


def parse_experience(text: Optional[str]) -> Dict[str, any]:
    """
    Parse experience text to structured fields.
    
    Examples:
        "0-2 years" → {'min': 0, 'max': 2, 'is_fresher': True}
        "2-5 yrs" → {'min': 2, 'max': 5, 'is_fresher': False}
        "Fresher" → {'min': 0, 'max': 0, 'is_fresher': True}
        "5+ years" → {'min': 5, 'max': None, 'is_fresher': False}
        "6 months" → {'min': 0.5, 'max': 0.5, 'is_fresher': True}
        NULL → {'min': None, 'max': None, 'is_fresher': False}
    
    Args:
        text: Experience requirement text
    
    Returns:
        Dictionary with min, max experience (float) and is_fresher (bool)
        Fresher definition: 0-6 months (0-0.5 years)
    """
    if not text or not text.strip():
        return {'min': None, 'max': None, 'is_fresher': False}
    
    text_lower = text.lower().strip()
    
    # Pattern 1: Check for "Fresher" variants (most specific first)
    if re.search(r'\b(?:fresh|fresher|freshers)\b', text_lower):
        return {'min': 0.0, 'max': 0.0, 'is_fresher': True}
    
    # Pattern 2: Check for months first
    # "6 months", "6-12 months", "6 to 12 months"
    month_range_match = re.search(r'(\d+)\s*(?:to|-)\s*(\d+)\s*months?', text_lower)
    if month_range_match:
        min_months = int(month_range_match.group(1))
        max_months = int(month_range_match.group(2))
        min_years = round(min_months / 12, 1)
        max_years = round(max_months / 12, 1)
        return {
            'min': min_years,
            'max': max_years,
            'is_fresher': max_years <= 0.5
        }
    
    # Single month value: "6 months", "3 months"
    single_month_match = re.search(r'(\d+)\s*months?', text_lower)
    if single_month_match:
        months = int(single_month_match.group(1))
        years = round(months / 12, 1)
        return {
            'min': years,
            'max': years,
            'is_fresher': years <= 0.5
        }
    
    # Pattern 3: Check for range "0-2 years", "2-5 yrs"
    range_match = re.search(r'(\d+)\s*(?:to|-)\s*(\d+)\s*(?:years?|yrs?)', text_lower)
    if range_match:
        min_exp = int(range_match.group(1))
        max_exp = int(range_match.group(2))
        return {
            'min': float(min_exp),
            'max': float(max_exp),
            'is_fresher': min_exp == 0 and max_exp <= 0.5
        }
    
    # Pattern 4: Check for "5+" pattern
    plus_match = re.search(r'(\d+)\+\s*(?:years?|yrs?)?', text_lower)
    if plus_match:
        min_exp = int(plus_match.group(1))
        return {
            'min': float(min_exp),
            'max': None,  # NULL means "no upper limit"
            'is_fresher': False
        }
    
    # Pattern 5: Check for single number "2 years", "3 yrs"
    single_match = re.search(r'(\d+)\s*(?:years?|yrs?)', text_lower)
    if single_match:
        exp = int(single_match.group(1))
        return {
            'min': float(exp),
            'max': float(exp),
            'is_fresher': exp == 0
        }
    
    # Pattern 6: Just "0" or "0 year"
    if re.search(r'\b0\b', text_lower):
        return {'min': 0.0, 'max': 0.0, 'is_fresher': True}
    
    # Pattern 7: "Experienced" (not fresh, but no specific number)
    if re.search(r'\bexperienced\b', text_lower):
        return {'min': None, 'max': None, 'is_fresher': False}
    
    # Pattern 8: Common phrases
    if re.search(r'not\s+(?:specified|required|applicable)', text_lower):
        return {'min': None, 'max': None, 'is_fresher': False}
    
    if re.search(r'any\s+experience', text_lower):
        return {'min': None, 'max': None, 'is_fresher': False}
    
    # Default: can't parse
    return {'min': None, 'max': None, 'is_fresher': False}


def extract_salary_from_text(text: Optional[str]) -> Dict[str, any]:
    """
    Extract salary information from job text (description or salary field).
    
    Looks for patterns like:
    - "Salary: 12-24 LPA"
    - "CTC: 3-4 LPA"
    - "Salary: $50k - $80k"
    - "Salary: ₹6-10 lakhs"
    
    Args:
        text: Text containing salary information
    
    Returns:
        Dictionary with min, max (int) and currency (str)
    """
    if not text:
        return {'min': None, 'max': None, 'currency': 'INR'}
    
    # Pattern 1: LPA (Lakhs Per Annum)
    # "12-24 LPA" or "3-4 LPA"
    lpa_match = re.search(r'(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*LPA', text, re.IGNORECASE)
    if lpa_match:
        min_lpa = float(lpa_match.group(1))
        max_lpa = float(lpa_match.group(2))
        # Convert LPA to actual rupees (1 LPA = 100,000)
        return {
            'min': int(min_lpa * 100000),
            'max': int(max_lpa * 100000),
            'currency': 'INR'
        }
    
    # Pattern 2: Dollar amounts
    # "$50k - $80k" or "$50,000 - $80,000"
    dollar_match = re.search(
        r'\$\s*(\d+(?:,\d+)?(?:\.\d+)?)\s*k?\s*-\s*\$?\s*(\d+(?:,\d+)?(?:\.\d+)?)\s*k?',
        text,
        re.IGNORECASE
    )
    if dollar_match:
        min_val = dollar_match.group(1).replace(',', '')
        max_val = dollar_match.group(2).replace(',', '')
        
        # Check if it's in thousands (k)
        if 'k' in dollar_match.group(0).lower():
            min_val = float(min_val) * 1000
            max_val = float(max_val) * 1000
        else:
            min_val = float(min_val)
            max_val = float(max_val)
        
        return {
            'min': int(min_val),
            'max': int(max_val),
            'currency': 'USD'
        }
    
    # Pattern 3: Lakhs (Indian currency)
    # "₹6-10 lakhs" or "6-10 lakhs"
    lakhs_match = re.search(
        r'₹?\s*(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)\s*lakhs?',
        text,
        re.IGNORECASE
    )
    if lakhs_match:
        min_lakhs = float(lakhs_match.group(1))
        max_lakhs = float(lakhs_match.group(2))
        # Convert lakhs to actual rupees (1 lakh = 100,000)
        return {
            'min': int(min_lakhs * 100000),
            'max': int(max_lakhs * 100000),
            'currency': 'INR'
        }
    
    return {'min': None, 'max': None, 'currency': 'INR'}


def parse_salary_from_jsonb(salary_range: Optional[dict]) -> Dict[str, any]:
    """
    Parse salary_range JSONB field to structured values.
    
    Args:
        salary_range: JSONB dict with min, max, currency keys
    
    Returns:
        Dictionary with min (int), max (int), currency (str)
    """
    if not salary_range or not isinstance(salary_range, dict):
        return {'min': None, 'max': None, 'currency': 'INR'}
    
    min_salary = salary_range.get('min')
    max_salary = salary_range.get('max')
    currency = salary_range.get('currency', 'INR')
    
    # Convert to integers if they're floats
    if min_salary is not None:
        min_salary = int(min_salary)
    if max_salary is not None:
        max_salary = int(max_salary)
    
    return {'min': min_salary, 'max': max_salary, 'currency': currency}
