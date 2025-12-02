# Fantasy Baseball Civil War

A fantasy baseball league tracking website that displays historical statistics, standings, awards, player scoring, and manager profiles for the Fantasy Baseball Civil War league (FBCW).

## Overview

This is a static HTML frontend that loads and displays fantasy baseball data from JSON files. The project was originally created in Google Colab and imported into Replit. It includes:

- **Frontend**: A single-page application (`index.html`) with multiple sections for viewing league data
- **Data Collection Scripts**: Python scripts for pulling data from Yahoo Fantasy API and Fangraphs
- **Static Assets**: Team logos, award images, league banners

## Project Structure

```
.
├── index.html              # Main frontend application
├── server.py              # Simple HTTP server to serve static files
├── data/                  # JSON data files
│   ├── current_season/    # Current season data (2025)
│   ├── historical/        # Historical season data (2019-2024)
│   ├── managers/          # Manager statistics and history
│   └── players/           # Player history
├── collect_data.py        # Yahoo API data collection script
├── app.py                 # Legacy data collection functions
├── auth_yahoo.py          # Yahoo OAuth authentication
└── *.png                  # Images and logos
```

## Features

- **Seasons View**: Browse standings and team information by season
- **Awards Section**: Display champions, runners-up, and other league awards
- **Player Scoring**: View league scoring settings and points breakdown
- **Manager Profiles**: Detailed career statistics for each manager with historical team data

## Running the Application

The application is automatically started via the configured workflow:
- The web server runs on port 5000
- Serves the static HTML frontend and JSON data files
- Includes cache-control headers to prevent stale data

## Data Collection

The `collect_data.py` script can be used to update league data:
- Pulls data from Yahoo Fantasy API using OAuth authentication
- Fetches player statistics from Fangraphs via pybaseball
- Requires `oauth2.json` file for Yahoo API authentication (not included in repo)

## Dependencies

Python dependencies (see `requirements.txt`):
- yahoo-oauth: Yahoo OAuth authentication
- yahoo-fantasy-api: Yahoo Fantasy Sports API wrapper
- pandas: Data manipulation and analysis
- pybaseball: Fangraphs data access

## Recent Changes

- **2024-12-02**: Imported from GitHub and configured for Replit environment
  - Created `server.py` to serve static files on port 5000
  - Added `requirements.txt` for Python dependencies
  - Configured workflow for automatic server startup
  - Added `.gitignore` for Python project files
  - Configured deployment settings for autoscale deployment

## Notes

- The frontend is fully static and can work without the Python backend
- Python scripts (`collect_data.py`, `app.py`) are for data collection only
- OAuth credentials are required to collect new data from Yahoo Fantasy API
- The application displays cached JSON data from the `data/` directory
