"""
Fantasy Baseball League Data Collection System
Automatically updates weekly during the season
"""

import json
import os
from datetime import datetime
from typing import List, Dict
from yahoo_oauth import OAuth2
from yahoo_fantasy_api import Game, League
import pandas as pd

# =============================================================================
# CONFIGURATION
# =============================================================================

CURRENT_SEASON = 2025
HISTORICAL_SEASONS = [2019, 2020, 2021, 2022, 2023, 2024]
DATA_DIR = "data"

# Override league IDs for specific years (if needed)
LEAGUE_ID_OVERRIDES = {
    2020: "398.l.17906"  # Use this specific league for 2020
}

# Manager name corrections - maps raw names to standardized names
MANAGER_NAME_MAP = {
    # Handles case variations (John, JOHN -> John)
    # Will be applied after title case conversion
}

# Manager team disambiguation for 2023 (Logan C vs Logan S)
MANAGER_TEAM_2023 = {
    "Draft Pool": "Logan C",
    "Peanut Butter & Elly": "Logan S"
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def normalize_manager_name(manager_name: str, year: int = None, team_name: str = None) -> str:
    """
    Normalize manager names to proper case and handle special cases
    """
    # First, convert to title case
    normalized = manager_name.strip().title()
    
    # Handle 2023 Logan disambiguation
    if year == 2023 and team_name:
        if team_name in MANAGER_TEAM_2023:
            return MANAGER_TEAM_2023[team_name]
    
    # Apply any custom name mappings
    if normalized in MANAGER_NAME_MAP:
        return MANAGER_NAME_MAP[normalized]
    
    return normalized

# =============================================================================
# SETUP & AUTHENTICATION
# =============================================================================

def setup_oauth():
    """
    Initialize OAuth for Yahoo Fantasy API
    Requires oauth2.json file with credentials
    """
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    oauth_path = os.path.join(script_dir, 'oauth2.json')
    
    if not os.path.exists(oauth_path):
        raise FileNotFoundError(f"oauth2.json not found at: {oauth_path}")
    
    oauth = OAuth2(None, None, from_file=oauth_path)
    return oauth

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def get_league_id_by_name(oauth, year: int) -> str:
    """Get league ID for a specific year"""
    # Check if we have a manual override for this year
    if year in LEAGUE_ID_OVERRIDES:
        print(f"  Using override league ID: {LEAGUE_ID_OVERRIDES[year]}")
        return LEAGUE_ID_OVERRIDES[year]
    
    # Otherwise, use the first league ID for the year
    gm = Game(oauth, 'mlb')
    league_ids = gm.league_ids(year=year)
    
    if league_ids:
        return league_ids[0]
    
    return None

# =============================================================================
# YOUR EXISTING FUNCTIONS (with minor updates)
# =============================================================================

def get_teams(oauth, year: int) -> List[Dict]:
    """Get all teams for a given season"""
    gm = Game(oauth, 'mlb')
    league_id = get_league_id_by_name(oauth, year)
    
    if not league_id:
        return []
    
    lg = League(oauth, league_id)

    standings = lg.standings()
    teams = lg.teams()

    keys = []
    for i in range(len(standings)):
        key = standings[i]['team_key']
        keys.append(key)

    tm = []
    for i in range(len(keys)):
        tm.append({
            'team_key': teams[keys[i]]['team_key'],
            'team_name': teams[keys[i]]['name'],
            'team_logo': teams[keys[i]]['team_logos'][0]['team_logo']['url'],
            'manager': teams[keys[i]]['managers'][0]['manager']['nickname']
        })

    return tm

def get_week_scores(oauth, year: int, week: int) -> List[Dict]:
    """Get matchup scores for a specific week"""
    gm = Game(oauth, 'mlb')
    league_id = get_league_id_by_name(oauth, year)
    
    if not league_id:
        return []
    
    lg = League(oauth, league_id)

    try:
        matchup_data = lg.matchups(week=week)
    except:
        return []

    data = []
    
    try:
        matchups = matchup_data['fantasy_content']['league'][1]['scoreboard']['0']['matchups']
        
        for i in range(0, 6):  # 6 matchups for 12 teams
            try:
                matchup_key = str(i)
                if matchup_key not in matchups:
                    break
                    
                matchup = matchups[matchup_key]['matchup']['0']['teams']
                
                team1 = matchup['0']['team']
                team2 = matchup['1']['team']
                
                team1_key = team1[0][0]['team_key']
                team1_score = team1[1].get('team_points', {}).get('total', 0)
                
                team2_key = team2[0][0]['team_key']
                team2_score = team2[1].get('team_points', {}).get('total', 0)
                
                week_num = team1[1].get('team_stats', {}).get('week', week)

                data.append({
                    'team_key': team1_key,
                    'team_score': float(team1_score),
                    'week': int(week_num),
                    'opponent_key': team2_key,
                    'opponent_score': float(team2_score)
                })

                data.append({
                    'team_key': team2_key,
                    'team_score': float(team2_score),
                    'week': int(week_num),
                    'opponent_key': team1_key,
                    'opponent_score': float(team1_score)
                })
            except (KeyError, IndexError) as e:
                # Skip this matchup if data structure is different
                continue
                
    except (KeyError, IndexError) as e:
        # If we can't parse matchups, return empty
        pass

    return data

def get_draft_results(oauth, year: int) -> List[Dict]:
    """Get draft results for a season"""
    gm = Game(oauth, 'mlb')
    league_id = get_league_id_by_name(oauth, year)
    
    if not league_id:
        return []
    
    lg = League(oauth, league_id)

    draft = lg.draft_results()
    return draft

def get_standings(oauth, year: int) -> List[Dict]:
    """Get full season standings"""
    gm = Game(oauth, 'mlb')
    league_id = get_league_id_by_name(oauth, year)
    
    if not league_id:
        return []
    
    lg = League(oauth, league_id)

    standings = lg.standings()
    teams = lg.teams()

    results = []
    for standing in standings:
        team_key = standing['team_key']
        team_info = teams[team_key]
        
        team_name = team_info.get('name', 'Unknown Team')
        raw_manager = team_info['managers'][0]['manager'].get('nickname', 'Unknown Manager')
        
        # Normalize manager name
        manager = normalize_manager_name(raw_manager, year, team_name)
        
        # Some older seasons may not have all fields
        results.append({
            'rank': int(standing.get('rank', 0)),
            'team_key': team_key,
            'team_name': team_name,
            'manager': manager,
            'wins': int(standing.get('outcome_totals', {}).get('wins', 0)),
            'losses': int(standing.get('outcome_totals', {}).get('losses', 0)),
            'ties': int(standing.get('outcome_totals', {}).get('ties', 0)),
            'win_pct': float(standing.get('outcome_totals', {}).get('percentage', 0)),
            'points_for': float(standing.get('points_for', 0)),
            'points_against': float(standing.get('points_against', 0))
        })
    
    return results

# =============================================================================
# NEW AGGREGATION FUNCTIONS
# =============================================================================

def get_all_season_scores(oauth, year: int, num_weeks: int = 24) -> List[Dict]:
    """Get all weekly scores for entire season"""
    all_scores = []
    
    for week in range(1, num_weeks + 1):
        try:
            week_data = get_week_scores(oauth, year, week)
            all_scores.extend(week_data)
            print(f"  ✓ Week {week} collected")
        except KeyError as e:
            print(f"  ✗ Week {week} not available (KeyError: {e})")
            break
        except Exception as e:
            print(f"  ✗ Week {week} not available: {type(e).__name__}: {e}")
            break
    
    return all_scores

def calculate_manager_stats(all_seasons_data: Dict) -> Dict:
    """Calculate all-time manager statistics"""
    manager_stats = {}
    
    for year, data in all_seasons_data.items():
        standings = data.get('standings', [])
        
        # Handle 2019 playoff corrections based on actual playoff results
        if year == 2019:
            standings = correct_2019_playoffs(data)
        
        for team in standings:
            manager_raw = team['manager']
            
            # Determine the actual manager name (handle Logan and Josh disambiguation)
            
            # Handle Logan disambiguation
            if manager_raw == "Logan":
                if year == 2023:
                    # Use team_key to distinguish in 2023
                    if team['team_key'] == "422.l.6780.t.4":
                        manager = "Logan C"
                    elif team['team_key'] == "422.l.6780.t.12":
                        manager = "Logan S"
                    else:
                        # Fallback to team name if team_key doesn't match
                        if "Draft Pool" in team['team_name']:
                            manager = "Logan C"
                        elif "Peanut Butter" in team['team_name'] or "Elly" in team['team_name']:
                            manager = "Logan S"
                        else:
                            manager = "Logan"  # Keep as is if can't determine
                elif year >= 2020 and year <= 2022:
                    manager = "Logan C"
                elif year >= 2024:
                    manager = "Logan S"
                else:
                    manager = "Logan"  # Default
            
            # Handle Josh disambiguation (2019-2022)
            elif manager_raw == "Josh":
                if year >= 2019 and year <= 2022:
                    # Josh B always has team_key ending in "t.1"
                    if team['team_key'].endswith('t.1'):
                        manager = "Josh B"
                    else:
                        manager = "Josh S"
                elif year >= 2023:
                    # After 2022, only Josh B remains (assuming he kept t.1)
                    if team['team_key'].endswith('t.1'):
                        manager = "Josh B"
                    else:
                        manager = "Josh"  # Default
                else:
                    manager = "Josh"  # Default
            
            else:
                manager = manager_raw
            
            if manager not in manager_stats:
                manager_stats[manager] = {
                    'manager_name': manager,
                    'first_season': year,
                    'total_wins': 0,
                    'total_losses': 0,
                    'total_ties': 0,
                    'championships': 0,
                    'runner_ups': 0,
                    'playoff_appearances': 0,
                    'seasons_played': 0,
                    'total_points_for': 0,
                    'season_history': []
                }
            
            manager_stats[manager]['total_wins'] += team['wins']
            manager_stats[manager]['total_losses'] += team['losses']
            manager_stats[manager]['total_ties'] += team['ties']
            manager_stats[manager]['total_points_for'] += team['points_for']
            manager_stats[manager]['seasons_played'] += 1
            
            if team['rank'] == 1:
                manager_stats[manager]['championships'] += 1
            if team['rank'] == 2:
                manager_stats[manager]['runner_ups'] += 1
            if team['rank'] <= 6:  # Assuming top 6 make playoffs
                manager_stats[manager]['playoff_appearances'] += 1
            
            manager_stats[manager]['season_history'].append({
                'year': year,
                'team_name': team['team_name'],
                'rank': team['rank'],
                'wins': team['wins'],
                'losses': team['losses'],
                'points_for': team['points_for']
            })
    
    # Calculate win percentages
    for manager in manager_stats.values():
        total_games = manager['total_wins'] + manager['total_losses']
        manager['win_pct'] = round(manager['total_wins'] / total_games, 3) if total_games > 0 else 0
        manager['avg_finish'] = round(
            sum(s['rank'] for s in manager['season_history']) / len(manager['season_history']), 1
        )
    
    return manager_stats

def correct_2019_playoffs(season_data: Dict) -> List[Dict]:
    """
    Correct 2019 standings based on actual playoff results
    Week 18-20 were playoff weeks
    Ryan defeated Rich in week 20 championship
    """
    standings = season_data.get('standings', [])
    
    # Manual corrections based on actual playoff results
    # Ryan won championship, Rich was runner-up
    CORRECT_2019_RANKS = {
        'Ryan': 1,    # Champion (defeated Rich in week 20)
        'Rich': 2,    # Runner-up (lost to Ryan in week 20)
        'Tyler': 3    # 3rd place
    }
    
    corrected_standings = []
    for team in standings:
        team_copy = team.copy()
        manager = team['manager']
        
        if manager in CORRECT_2019_RANKS:
            old_rank = team_copy['rank']
            team_copy['rank'] = CORRECT_2019_RANKS[manager]
            print(f"  ✓ 2019 {manager}: rank {old_rank} → {team_copy['rank']}")
        
        corrected_standings.append(team_copy)
    
    # Sort by corrected rank
    corrected_standings.sort(key=lambda x: x['rank'])
    
    return corrected_standings

# =============================================================================
# DATA COLLECTION & EXPORT FUNCTIONS
# =============================================================================

def create_directory_structure():
    """Create necessary directories"""
    dirs = [
        DATA_DIR,
        f"{DATA_DIR}/current_season",
        f"{DATA_DIR}/historical",
        f"{DATA_DIR}/managers"
    ]
    
    for directory in dirs:
        os.makedirs(directory, exist_ok=True)

def collect_historical_data(oauth):
    """Collect all historical season data (run once)"""
    print("Collecting historical data...")
    
    all_seasons = {}
    
    for year in HISTORICAL_SEASONS:
        print(f"\nCollecting {year} season...")
        
        # Get standings
        standings = get_standings(oauth, year)
        
        # Get all weekly scores
        scores = get_all_season_scores(oauth, year)
        
        # Get draft results
        try:
            draft = get_draft_results(oauth, year)
        except:
            draft = []
        
        # Get teams
        teams = get_teams(oauth, year)
        
        season_data = {
            'year': year,
            'standings': standings,
            'scores': scores,
            'draft': draft,
            'teams': teams
        }
        
        all_seasons[year] = season_data
        
        # Save to file
        year_dir = f"{DATA_DIR}/historical/{year}"
        os.makedirs(year_dir, exist_ok=True)
        
        with open(f"{year_dir}/final_standings.json", 'w') as f:
            json.dump(standings, f, indent=2)
        
        with open(f"{year_dir}/all_scores.json", 'w') as f:
            json.dump(scores, f, indent=2)
        
        with open(f"{year_dir}/draft.json", 'w') as f:
            json.dump(draft, f, indent=2)
        
        with open(f"{year_dir}/teams.json", 'w') as f:
            json.dump(teams, f, indent=2)
        
        print(f"✓ {year} season saved")
    
    return all_seasons

def collect_current_season_data(oauth):
    """Collect current season data (run weekly)"""
    print(f"\nUpdating {CURRENT_SEASON} season data...")
    
    # Create current_season directory if it doesn't exist
    current_season_dir = f"{DATA_DIR}/current_season"
    os.makedirs(current_season_dir, exist_ok=True)
    
    # Get current standings
    standings = get_standings(oauth, CURRENT_SEASON)
    
    # Get teams
    teams = get_teams(oauth, CURRENT_SEASON)
    
    # Get all weeks so far (will stop when it hits a week not yet played)
    scores = get_all_season_scores(oauth, CURRENT_SEASON, num_weeks=26)
    
    # Save current season data
    with open(f"{current_season_dir}/standings.json", 'w') as f:
        json.dump(standings, f, indent=2)
    
    with open(f"{current_season_dir}/teams.json", 'w') as f:
        json.dump(teams, f, indent=2)
    
    with open(f"{current_season_dir}/all_scores.json", 'w') as f:
        json.dump(scores, f, indent=2)
    
    # Organize scores by week for easy access
    scores_by_week = {}
    for score in scores:
        week = score['week']
        if week not in scores_by_week:
            scores_by_week[week] = []
        scores_by_week[week].append(score)
    
    for week, week_scores in scores_by_week.items():
        with open(f"{current_season_dir}/week_{week}_scores.json", 'w') as f:
            json.dump(week_scores, f, indent=2)
    
    print(f"✓ Current season updated ({len(scores_by_week)} weeks)")
    
    return {
        'year': CURRENT_SEASON,
        'standings': standings,
        'scores': scores,
        'teams': teams
    }

def update_manager_stats(oauth):
    """Update manager statistics including current season"""
    print("\nUpdating manager statistics...")
    
    # Load all historical data with scores
    all_seasons = {}
    
    for year in HISTORICAL_SEASONS:
        year_dir = f"{DATA_DIR}/historical/{year}"
        standings_file = f"{year_dir}/final_standings.json"
        scores_file = f"{year_dir}/all_scores.json"
        
        if os.path.exists(standings_file):
            with open(standings_file, 'r') as f:
                standings = json.load(f)
            
            scores = []
            if os.path.exists(scores_file):
                with open(scores_file, 'r') as f:
                    scores = json.load(f)
            
            all_seasons[year] = {
                'standings': standings,
                'scores': scores
            }
            print(f"  ✓ Loaded {year} season")
        else:
            print(f"  ⚠ Skipping {year} - no data found")
    
    # Add current season
    current_standings_file = f"{DATA_DIR}/current_season/standings.json"
    current_scores_file = f"{DATA_DIR}/current_season/all_scores.json"
    
    if os.path.exists(current_standings_file):
        with open(current_standings_file, 'r') as f:
            standings = json.load(f)
        
        scores = []
        if os.path.exists(current_scores_file):
            with open(current_scores_file, 'r') as f:
                scores = json.load(f)
        
        all_seasons[CURRENT_SEASON] = {
            'standings': standings,
            'scores': scores
        }
        print(f"  ✓ Loaded {CURRENT_SEASON} season")
    else:
        print(f"  ⚠ Current season data not found yet")
    
    # Check if we have any data
    if not all_seasons:
        print("  ⚠ No season data available to calculate manager stats")
        return
    
    # Calculate stats
    manager_stats = calculate_manager_stats(all_seasons)
    
    # Create managers directory if it doesn't exist
    managers_dir = f"{DATA_DIR}/managers"
    os.makedirs(managers_dir, exist_ok=True)
    
    # Save manager stats
    with open(f"{managers_dir}/all_time_stats.json", 'w') as f:
        json.dump(list(manager_stats.values()), f, indent=2)
    
    # Save detailed history
    manager_history = []
    for manager_data in manager_stats.values():
        for season in manager_data['season_history']:
            manager_history.append({
                'manager': manager_data['manager_name'],
                **season
            })
    
    with open(f"{managers_dir}/manager_history.json", 'w') as f:
        json.dump(manager_history, f, indent=2)
    
    print(f"✓ Manager stats updated ({len(manager_stats)} managers)")

def create_league_info():
    """Create league metadata file"""
    league_info = {
        'league_name': 'Your League Name',  # Update this
        'founded': min(HISTORICAL_SEASONS) if HISTORICAL_SEASONS else CURRENT_SEASON,
        'current_season': CURRENT_SEASON,
        'total_teams': 12,
        'league_type': 'Points',
        'last_updated': datetime.now().isoformat()
    }
    
    with open(f"{DATA_DIR}/league_info.json", 'w') as f:
        json.dump(league_info, f, indent=2)

# =============================================================================
# MAIN EXECUTION FUNCTIONS
# =============================================================================

def initial_setup():
    """Run this ONCE to collect all historical data"""
    print("=" * 60)
    print("INITIAL SETUP - Collecting All Historical Data")
    print("=" * 60)
    
    create_directory_structure()
    oauth = setup_oauth()
    
    # Collect all historical seasons
    if HISTORICAL_SEASONS:
        collect_historical_data(oauth)
    else:
        print("\nNo historical seasons configured, skipping...")
    
    # Collect current season
    collect_current_season_data(oauth)
    
    # Calculate manager stats (after data is collected)
    update_manager_stats(oauth)
    
    # Create league info
    create_league_info()
    
    print("\n" + "=" * 60)
    print("✓ Initial setup complete!")
    print("=" * 60)

def weekly_update():
    """Run this WEEKLY during the season"""
    print("=" * 60)
    print(f"WEEKLY UPDATE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    oauth = setup_oauth()
    
    # Update current season data
    collect_current_season_data(oauth)
    
    # Update manager stats
    update_manager_stats(oauth)
    
    # Update league info timestamp
    create_league_info()
    
    print("\n" + "=" * 60)
    print("✓ Weekly update complete!")
    print("=" * 60)

# =============================================================================
# UTILITY: CHECK AVAILABLE SEASONS
# =============================================================================

def check_available_seasons():
    """Check which seasons your Yahoo account has league data for"""
    print("=" * 60)
    print("CHECKING AVAILABLE SEASONS AND LEAGUE NAMES")
    print("=" * 60)
    
    oauth = setup_oauth()
    gm = Game(oauth, 'mlb')
    
    available_years = []
    test_years = range(2015, 2026)  # Test from 2015 to 2025
    
    for year in test_years:
        try:
            league_ids = gm.league_ids(year=year)
            if league_ids:
                print(f"\n{year}:")
                for league_id in league_ids:
                    try:
                        lg = League(oauth, league_id)
                        league_metadata = lg.metadata()
                        league_name = league_metadata.get('name', 'Unknown')
                        print(f"  - '{league_name}' (ID: {league_id})")
                        available_years.append(year)
                    except:
                        print(f"  - ID: {league_id} (couldn't get name)")
            else:
                print(f"{year}: No leagues found")
        except Exception as e:
            print(f"{year}: No leagues found")
    
    print("\n" + "=" * 60)
    print("Copy the EXACT league name and update LEAGUE_NAME_FILTER in collect_data.py")
    print("=" * 60)
    
    return available_years

# =============================================================================
# RUN THE APPROPRIATE FUNCTION
# =============================================================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "setup":
        initial_setup()
    elif len(sys.argv) > 1 and sys.argv[1] == "check":
        check_available_seasons()
    else:
        weekly_update()
