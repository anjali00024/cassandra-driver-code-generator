#!/bin/bash

# Start the backend server
echo "Starting backend server..."
cd backend
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8000 &

# Start the frontend server
echo "Starting frontend server..."
cd ..
npm install
npm start 