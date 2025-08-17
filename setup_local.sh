#!/bin/bash
# FOR LOCAL DEVELOPMENT
# Set the PYTHONPATH to the current directory (project root)
export PYTHONPATH=$(pwd)
echo "PYTHONPATH set to: $PYTHONPATH"

# Activate virtual environment
source venv/bin/activate
echo "Virtual environment activated"

# Start the Python backend in the background
echo "Starting backend server..."
python -m backend.app &

echo "✅ Server running. Your application is ready to use."

# Wait for background processes to exit
wait