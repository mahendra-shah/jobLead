#!/bin/bash

# Run database migrations for job processing pipeline

set -e

echo "🗃️  Running Database Migrations"
echo "=============================="
echo ""

# Run migration
echo "📤 Running alembic upgrade..."
docker-compose exec -T backend alembic upgrade head

echo ""
echo "✅ Migration complete!"
echo ""

# Verify table was created
echo "🔍 Verifying job_scraping_preferences table..."
docker-compose exec -T backend psql -U postgres -h host.docker.internal -d placement_db \
    -c "\d job_scraping_preferences"

echo ""

# Show default preferences
echo "📋 Default Preferences:"
docker-compose exec -T backend psql -U postgres -h host.docker.internal -d placement_db \
    -c "SELECT allowed_job_types, min_experience_years, max_experience_years, priority_skills, excluded_keywords, min_ai_confidence_score, max_messages_per_run, is_active FROM job_scraping_preferences;"

echo ""
echo "🎉 All done!"
echo ""
echo "📝 Next steps:"
echo "  1. Setup DynamoDB: ./scripts/setup_dynamodb.sh"
echo "  2. Test preferences API: curl http://localhost:8000/api/v1/admin/job-preferences"
echo ""
