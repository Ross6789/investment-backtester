#!/bin/bash

# Set the PYTHONPATH to the current directory (project root)
export PYTHONPATH=$(pwd)

echo "PYTHONPATH set to: $PYTHONPATH"

# Optional: activate virtual environment if you want to automate that too
source venv/bin/activate
echo "Virtual environment activated"

echo "You're now ready to run your Python scripts."