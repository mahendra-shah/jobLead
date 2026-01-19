"""Common constants."""

# Job types
JOB_TYPES = ["remote", "office", "hybrid"]

# Employment types
EMPLOYMENT_TYPES = ["fulltime", "parttime", "contract", "freelance", "internship"]

# Application statuses
APPLICATION_STATUSES = [
    "applied",
    "viewed",
    "shortlisted",
    "interviewed",
    "offered",
    "rejected",
    "accepted",
    "withdrawn",
]

# Student statuses
STUDENT_STATUSES = ["active", "placed", "inactive"]

# Company verification statuses
COMPANY_VERIFICATION_STATUSES = ["verified", "unverified", "blacklisted"]

# User roles
USER_ROLES = ["superadmin", "admin", "placement", "student", "employer"]

# File extensions
ALLOWED_RESUME_EXTENSIONS = ["pdf", "docx"]

# Experience levels
EXPERIENCE_LEVELS = [
    "0-1 years",
    "1-2 years",
    "2-5 years",
    "5-10 years",
    "10+ years",
]

# Skills categories
SKILL_CATEGORIES = {
    "programming": [
        "Python",
        "Java",
        "JavaScript",
        "TypeScript",
        "C++",
        "Go",
        "Rust",
        "PHP",
        "Ruby",
    ],
    "web": ["React", "Angular", "Vue", "Next.js", "Node.js", "Django", "FastAPI", "Flask"],
    "mobile": ["React Native", "Flutter", "Swift", "Kotlin", "Android", "iOS"],
    "database": ["PostgreSQL", "MySQL", "MongoDB", "Redis", "DynamoDB", "Elasticsearch"],
    "devops": ["Docker", "Kubernetes", "AWS", "GCP", "Azure", "CI/CD", "Jenkins", "GitLab"],
    "ml": ["TensorFlow", "PyTorch", "scikit-learn", "NLP", "Computer Vision"],
}
