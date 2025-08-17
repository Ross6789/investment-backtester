#!/usr/bin/env bash
# Make sure we run on the port Render provides
export PORT=${PORT:-10000}  # default fallback

# Run Flask in production mode
export FLASK_APP=backend.app
export FLASK_ENV=production

# Start Flask using gunicorn for production
gunicorn backend.app:app --bind 0.0.0.0:$PORT
