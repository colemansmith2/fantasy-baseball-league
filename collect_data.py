"""
Fantasy Baseball League Data Collection System
Automatically updates weekly during the season

Uses Yahoo Fantasy API for:
- Rosters (who owns which player)
- Team info (logos, manager names)
- Transactions
- Scoring settings

Uses Fangraphs (via pybaseball) for:
- Player batting stats
- Player pitching stats
"""

import json
import os
import re
import time
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from yahoo_oauth import OAuth2
from yahoo_fantasy_api import Game, League
import pandas as pd

# Try to import pybaseball - will be used for Fangraphs data
try:
    from pybaseball import batting_stats, pitching_stats, cache
    # Enable caching to avoid repeated requests
    cache.enable()
    PYBASEBALL_AVAILABLE = True
except ImportError:
    PYBASEBALL_AVAILABLE = False
    print("WARNING: pybaseball not installed. Install with: pip install pybaseball")

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
# DEFAULT SCORING SETTINGS (Yahoo Points League defaults)
# These will be overwritten by actual league settings if available
# =============================================================================

DEFAULT_BATTING_SCORING = {
    '1B': 2.6,      # Singles
    '2B': 5.2,      # Doubles
    '3B': 7.8,      # Triples
    'HR': 10.4,     # Home Runs
    'RBI': 1.9,     # Runs Batted In
    'R': 1.9,       # Runs
    'BB': 2.6,      # Walks
    'HBP': 2.6,     # Hit By Pitch
    'SB': 4.2,      # Stolen Bases
    'CS': -2.6,     # Caught Stealing
    'SO': -1,       # Strikeouts (batting)
}

DEFAULT_PITCHING_SCORING = {
    'IP': 5,        # Innings Pitched (per full inning)
    'W': 4,         # Wins
    'L': -4,        # Losses
    'SV': 8,        # Saves
    'HLD': 4,       # Holds
    'ER': -3,       # Earned Runs
    'H': -1,        # Hits Allowed
    'BB': -1,       # Walks Allowed
    'K': 3,         # Strikeouts (pitching)
    'QS': 4,        # Quality Starts
    'CG': 5,        # Complete Games
    'SO': 5,        # Shutouts
}

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def normalize_manager_name(manager_name: str, year: int = None, team_name: str = None) -> str:
    """Normalize manager names to proper case and handle special cases"""
    normalized = manager_name.strip().title()
    
    if year == 2023 and team_name:
        if team_name in MANAGER_TEAM_2023:
            return MANAGER_TEAM_2023[team_name]
    
    if normalized in MANAGER_NAME_MAP:
        return MANAGER_NAME_MAP[normalized]
    
    return normalized

def normalize_player_name(name: str) -> str:
    """Normalize player names for matching between Yahoo and Fangraphs."""
    if not name:
        return ""
    
    # Remove common suffixes/parentheticals
    name = re.sub(r'\s*\(Batter\)|\s*\(Pitcher\)', '', name, flags=re.IGNORECASE)
    
    # Remove accents
    accent_map = {
        'á': 'a', 'é': 'e', 'í': 'i', 'ó': 'o', 'ú': 'u',
        'ñ': 'n', 'ü': 'u', 'Á': 'A', 'É': 'E', 'Í': 'I',
        'Ó': 'O', 'Ú': 'U', 'Ñ': 'N', 'Ü': 'U'
    }
    for accented, plain in accent_map.items():
        name = name.replace(accented, plain)
    
    name = ' '.join(name.split())
    return name.lower().strip()

def match_player_name(yahoo_name: str, fangraphs_names: List[str]) -> Optional[str]:
    """Find the best matching Fangraphs name for a Yahoo player name."""
    normalized_yahoo = normalize_player_name(yahoo_name)
    
    # Exact match
    for fg_name in fangraphs_names:
        if normalize_player_name(fg_name) == normalized_yahoo:
            return fg_name
    
    # Match without Jr., Sr., II, III, etc.
    suffix_pattern = r'\s+(jr\.?|sr\.?|ii|iii|iv)$'
    yahoo_no_suffix = re.sub(suffix_pattern, '', normalized_yahoo, flags=re.IGNORECASE)
    
    for fg_name in fangraphs_names:
        fg_normalized = normalize_player_name(fg_name)
        fg_no_suffix = re.sub(suffix_pattern, '', fg_normalized, flags=re.IGNORECASE)
        
        if yahoo_no_suffix == fg_no_suffix:
            return fg_name
    
    # Match last name + first initial
    yahoo_parts = normalized_yahoo.split()
    if len(yahoo_parts) >= 2:
        yahoo_last = yahoo_parts[-1].rstrip('.')
        yahoo_first_initial = yahoo_parts[0][0] if yahoo_parts[0] else ''
        
        for fg_name in fangraphs_names:
            fg_normalized = normalize_player_name(fg_name)
            fg_parts = fg_normalized.split()
            if len(fg_parts) >= 2:
                fg_last = fg_parts[-1].rstrip('.')
                fg_first_initial = fg_parts[0][0] if fg_parts[0] else ''
                
                if yahoo_last == fg_last and yahoo_first_initial == fg_first_initial:
                    return fg_name
    
    return None

# =============================================================================
# SETUP & AUTHENTICATION
# =============================================================================

def setup_oauth():
    """Initialize OAuth for Yahoo Fantasy API"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    oauth_path = os.path.join(script_dir, 'oauth2.json')
    
    if not os.path.exists(oauth_path):
        raise FileNotFoundError(f"oauth2.json not found at: {oauth_path}")
    
    oauth = OAuth2(None, None, from_file=oauth_path)
    return oauth

def get_league_id_by_name(oauth, year: int) -> str:
    """Get league ID for a specific year"""
    if year in LEAGUE_ID_OVERRIDES:
        print(f"  Using override league ID: {LEAGUE_ID_OVERRIDES[year]}")
        return LEAGUE_ID_OVERRIDES[year]
    
    gm = Game(oauth, 'mlb')
    league_ids = gm.league_ids(year=year)
    
    if league_ids:
        return league_ids[0]
    return None

# =============================================================================
# YAHOO API DATA FUNCTIONS
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

    keys = [standings[i]['team_key'] for i in range(len(standings))]
    
    tm = []
    for key in keys:
        tm.append({
            'team_key': teams[key]['team_key'],
            'team_name': teams[key]['name'],
            'team_logo': teams[key]['team_logos'][0]['team_logo']['url'],
            'manager': teams[key]['managers'][0]['manager']['nickname']
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
        
        for i in range(0, 6):
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
            except (KeyError, IndexError):
                continue
    except (KeyError, IndexError):
        pass

    return data

def get_draft_results(oauth, year: int) -> List[Dict]:
    """Get draft results for a season"""
    gm = Game(oauth, 'mlb')
    league_id = get_league_id_by_name(oauth, year)
    
    if not league_id:
        return []
    
    lg = League(oauth, league_id)
    return lg.draft_results()

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
        manager = normalize_manager_name(raw_manager, year, team_name)
        
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

def get_all_season_scores(oauth, year: int, num_weeks: int = 24) -> List[Dict]:
    """Get all weekly scores for entire season"""
    all_scores = []
    
    for week in range(1, num_weeks + 1):
        try:
            week_data = get_week_scores(oauth, year, week)
            all_scores.extend(week_data)
            print(f"  ✓ Week {week} collected")
        except Exception as e:
            print(f"  ✗ Week {week} not available: {e}")
            break
    
    return all_scores

def get_rosters(oauth, year: int) -> Dict[str, List[Dict]]:
    """Get all team rosters for a given season."""
    gm = Game(oauth, 'mlb')
    league_id = get_league_id_by_name(oauth, year)
    
    if not league_id:
        return {}
    
    lg = League(oauth, league_id)
    teams_data = lg.teams()
    
    rosters = {}
    
    for team_key, team_info in teams_data.items():
        team_name = team_info.get('name', 'Unknown Team')
        raw_manager = team_info['managers'][0]['manager'].get('nickname', 'Unknown Manager')
        manager = normalize_manager_name(raw_manager, year, team_name)
        team_logo = team_info.get('team_logos', [{}])[0].get('team_logo', {}).get('url', '')
        
        print(f"    Getting roster for {team_name}...")
        
        try:
            team_obj = lg.to_team(team_key)
            roster = team_obj.roster(week=None)
            
            players = []
            for player in roster:
                player_id = player.get('player_id', '')
                name = player.get('name', '')
                if isinstance(name, dict):
                    name = name.get('full', '')
                
                position_type = player.get('position_type', '')
                eligible_positions = player.get('eligible_positions', [])
                if isinstance(eligible_positions, str):
                    eligible_positions = [eligible_positions]
                
                primary_position = eligible_positions[0] if eligible_positions else ''
                
                players.append({
                    'player_id': player_id,
                    'name': name,
                    'position_type': position_type,
                    'eligible_positions': eligible_positions,
                    'primary_position': primary_position,
                    'selected_position': player.get('selected_position', ''),
                    'status': player.get('status', ''),
                    'team_key': team_key,
                    'team_name': team_name,
                    'team_logo': team_logo,
                    'manager': manager,
                })
            
            rosters[team_key] = players
            
        except Exception as e:
            print(f"    ⚠ Could not get roster for {team_name}: {e}")
            rosters[team_key] = []
    
    return rosters

def get_league_scoring_settings(oauth, year: int) -> Dict:
    """Fetch league scoring settings."""
    gm = Game(oauth, 'mlb')
    league_id = get_league_id_by_name(oauth, year)
    
    if not league_id:
        return {'batting': DEFAULT_BATTING_SCORING, 'pitching': DEFAULT_PITCHING_SCORING}
    
    lg = League(oauth, league_id)
    
    try:
        settings = lg.settings()
        stat_categories = settings.get('stat_categories', {}).get('stats', [])
        
        batting_scoring = {}
        pitching_scoring = {}
        
        stat_id_map = {
            '7': 'R', '8': 'H', '9': '1B', '10': '2B', '11': '3B', '12': 'HR',
            '13': 'RBI', '16': 'SB', '17': 'CS', '18': 'BB', '21': 'SO', '51': 'HBP',
            '28': 'IP', '32': 'ER', '34': 'HA', '35': 'BBA', '42': 'K',
            '37': 'W', '38': 'L', '39': 'SV', '48': 'HLD', '57': 'QS',
        }
        
        for stat in stat_categories:
            stat_info = stat.get('stat', {})
            stat_id = str(stat_info.get('stat_id', ''))
            point_value = stat_info.get('value', 0)
            position_type = stat_info.get('position_type', '')
            
            if stat_id in stat_id_map and point_value:
                stat_name = stat_id_map[stat_id]
                if position_type == 'B':
                    batting_scoring[stat_name] = float(point_value)
                elif position_type == 'P':
                    pitching_scoring[stat_name] = float(point_value)
        
        for stat, value in DEFAULT_BATTING_SCORING.items():
            if stat not in batting_scoring:
                batting_scoring[stat] = value
        
        for stat, value in DEFAULT_PITCHING_SCORING.items():
            if stat not in pitching_scoring:
                pitching_scoring[stat] = value
        
        return {
            'batting': batting_scoring,
            'pitching': pitching_scoring,
            'raw_settings': stat_categories
        }
        
    except Exception as e:
        print(f"  ⚠ Could not get scoring settings, using defaults: {e}")
        return {'batting': DEFAULT_BATTING_SCORING, 'pitching': DEFAULT_PITCHING_SCORING}

def get_all_transactions(oauth, year: int, max_per_type: int = 1000) -> List[Dict]:
    """Fetch all transactions for the league."""
    gm = Game(oauth, 'mlb')
    league_id = get_league_id_by_name(oauth, year)
    
    if not league_id:
        return []
    
    lg = League(oauth, league_id)
    
    try:
        trans_types = ['add', 'drop', 'add/drop', 'trade']
        all_transactions = []
        
        for trans_type in trans_types:
            try:
                print(f"    Fetching {trans_type} transactions...")
                transactions_raw = lg.transactions(trans_type, count=max_per_type)
                
                if not transactions_raw:
                    print(f"      ✓ 0 {trans_type} transactions")
                    continue
                
                type_count = 0
                for trans in transactions_raw:
                    trans_data = {
                        'transaction_key': trans.get('transaction_key', ''),
                        'transaction_id': trans.get('transaction_id', ''),
                        'type': trans.get('type', trans_type),
                        'timestamp': trans.get('timestamp', ''),
                        'status': trans.get('status', ''),
                        'players': []
                    }
                    
                    players = trans.get('players', [])
                    if isinstance(players, dict):
                        player_count = players.get('count', 0)
                        for i in range(int(player_count)):
                            player_info = players.get(str(i), {})
                            if player_info:
                                player = player_info.get('player', [[{}]])
                                if isinstance(player, list) and len(player) > 0:
                                    player_data = player[0]
                                    if isinstance(player_data, list) and len(player_data) > 0:
                                        player_data = player_data[0]
                                else:
                                    player_data = player
                                
                                trans_player_data = player_info.get('transaction_data', {})
                                if isinstance(trans_player_data, list) and len(trans_player_data) > 0:
                                    trans_player_data = trans_player_data[0]
                                
                                player_name = ''
                                if isinstance(player_data, dict):
                                    name_info = player_data.get('name', {})
                                    if isinstance(name_info, dict):
                                        player_name = name_info.get('full', '')
                                    elif isinstance(name_info, str):
                                        player_name = name_info
                                
                                player_trans = {
                                    'player_key': player_data.get('player_key', '') if isinstance(player_data, dict) else '',
                                    'player_name': player_name,
                                    'transaction_type': trans_player_data.get('type', '') if isinstance(trans_player_data, dict) else '',
                                    'source_type': trans_player_data.get('source_type', '') if isinstance(trans_player_data, dict) else '',
                                    'source_team_key': trans_player_data.get('source_team_key', '') if isinstance(trans_player_data, dict) else '',
                                    'source_team_name': trans_player_data.get('source_team_name', '') if isinstance(trans_player_data, dict) else '',
                                    'destination_team_key': trans_player_data.get('destination_team_key', '') if isinstance(trans_player_data, dict) else '',
                                    'destination_team_name': trans_player_data.get('destination_team_name', '') if isinstance(trans_player_data, dict) else '',
                                }
                                trans_data['players'].append(player_trans)
                    
                    all_transactions.append(trans_data)
                    type_count += 1
                
                print(f"      ✓ {type_count} {trans_type} transactions")
                    
            except Exception as e:
                print(f"    ⚠ Could not get {trans_type} transactions: {e}")
                continue
        
        all_transactions.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        print(f"    Total: {len(all_transactions)} transactions")
        return all_transactions
        
    except Exception as e:
        print(f"  ⚠ Could not get transactions: {e}")
        return []

# =============================================================================
# FANGRAPHS DATA FUNCTIONS (via pybaseball)
# =============================================================================

def get_fangraphs_batting_stats(year: int, timeout: int = 120) -> pd.DataFrame:
    """Fetch batting stats from Fangraphs for a given year."""
    if not PYBASEBALL_AVAILABLE:
        print("  ⚠ pybaseball not available")
        return pd.DataFrame()
    
    import signal
    import sys
    
    def timeout_handler(signum, frame):
        raise TimeoutError(f"Fangraphs batting request timed out after {timeout}s")
    
    try:
        print(f"    Fetching Fangraphs batting stats for {year}...")
        print(f"    (This may take 1-2 minutes on first run...)")
        sys.stdout.flush()
        
        # Set timeout (Unix only - Windows will skip this)
        if hasattr(signal, 'SIGALRM'):
            old_handler = signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout)
        
        try:
            batters = batting_stats(year, qual=1)
        finally:
            # Reset alarm
            if hasattr(signal, 'SIGALRM'):
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)
        
        print(f"    ✓ Got {len(batters)} batters from Fangraphs")
        return batters
    except TimeoutError as e:
        print(f"    ⚠ {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"    ⚠ Could not fetch Fangraphs batting stats: {e}")
        return pd.DataFrame()

def get_fangraphs_pitching_stats(year: int, timeout: int = 120) -> pd.DataFrame:
    """Fetch pitching stats from Fangraphs for a given year."""
    if not PYBASEBALL_AVAILABLE:
        print("  ⚠ pybaseball not available")
        return pd.DataFrame()
    
    import signal
    import sys
    
    def timeout_handler(signum, frame):
        raise TimeoutError(f"Fangraphs pitching request timed out after {timeout}s")
    
    try:
        print(f"    Fetching Fangraphs pitching stats for {year}...")
        print(f"    (This may take 1-2 minutes on first run...)")
        sys.stdout.flush()
        
        # Set timeout (Unix only - Windows will skip this)
        if hasattr(signal, 'SIGALRM'):
            old_handler = signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout)
        
        try:
            pitchers = pitching_stats(year, qual=1)
        finally:
            # Reset alarm
            if hasattr(signal, 'SIGALRM'):
                signal.alarm(0)
                signal.signal(signal.SIGALRM, old_handler)
        
        print(f"    ✓ Got {len(pitchers)} pitchers from Fangraphs")
        return pitchers
    except TimeoutError as e:
        print(f"    ⚠ {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"    ⚠ Could not fetch Fangraphs pitching stats: {e}")
        return pd.DataFrame()

def calculate_batting_fantasy_points(stats: Dict, scoring: Dict) -> float:
    """Calculate fantasy points for a batter."""
    points = 0.0
    
    if '1B' not in stats and 'H' in stats:
        doubles = stats.get('2B', 0) or 0
        triples = stats.get('3B', 0) or 0
        hrs = stats.get('HR', 0) or 0
        hits = stats.get('H', 0) or 0
        stats['1B'] = hits - doubles - triples - hrs
    
    for stat, value in scoring.items():
        if stat in stats and stats[stat] is not None:
            stat_value = float(stats[stat]) if stats[stat] else 0
            points += stat_value * value
    
    return round(points, 1)

def calculate_pitching_fantasy_points(stats: Dict, scoring: Dict) -> float:
    """Calculate fantasy points for a pitcher."""
    points = 0.0
    
    stat_mapping = {
        'IP': 'IP', 'W': 'W', 'L': 'L', 'SV': 'SV', 'HLD': 'HLD',
        'ER': 'ER', 'H': 'HA', 'BB': 'BBA', 'SO': 'K', 'QS': 'QS',
        'CG': 'CG', 'ShO': 'SO',
    }
    
    for fg_stat, score_stat in stat_mapping.items():
        if fg_stat in stats and stats[fg_stat] is not None:
            stat_value = float(stats[fg_stat]) if stats[fg_stat] else 0
            if score_stat in scoring:
                points += stat_value * scoring[score_stat]
    
    return round(points, 1)

# =============================================================================
# PLAYER STATS INTEGRATION
# =============================================================================

def build_player_stats(oauth, year: int) -> List[Dict]:
    """Build complete player stats by combining Yahoo rosters with Fangraphs stats."""
    print(f"\n  Building player stats for {year}...")
    
    # Get rosters from Yahoo
    print(f"  Step 1: Fetching rosters from Yahoo...")
    rosters = get_rosters(oauth, year)
    
    all_players = []
    for team_key, players in rosters.items():
        all_players.extend(players)
    
    print(f"    ✓ Got {len(all_players)} rostered players from Yahoo")
    
    # Get scoring settings
    print(f"  Step 2: Fetching scoring settings...")
    scoring = get_league_scoring_settings(oauth, year)
    batting_scoring = scoring.get('batting', DEFAULT_BATTING_SCORING)
    pitching_scoring = scoring.get('pitching', DEFAULT_PITCHING_SCORING)
    
    # Get Fangraphs stats
    print(f"  Step 3: Fetching stats from Fangraphs...")
    batting_df = get_fangraphs_batting_stats(year)
    pitching_df = get_fangraphs_pitching_stats(year)
    
    if batting_df.empty and pitching_df.empty:
        print("  ⚠ No Fangraphs data available, returning roster-only data")
        return all_players
    
    batting_names = batting_df['Name'].tolist() if not batting_df.empty else []
    pitching_names = pitching_df['Name'].tolist() if not pitching_df.empty else []
    
    # Match players and add stats
    print(f"  Step 4: Matching players and calculating fantasy points...")
    matched_count = 0
    unmatched_players = []
    
    for player in all_players:
        player_name = player.get('name', '')
        position_type = player.get('position_type', '')
        
        player['stats'] = {}
        player['fantasy_points'] = 0
        player['headshot_url'] = ''
        player['mlb_team'] = ''
        
        if position_type == 'B':
            fg_name = match_player_name(player_name, batting_names)
            if fg_name and not batting_df.empty:
                player_row = batting_df[batting_df['Name'] == fg_name].iloc[0]
                
                stats = {
                    'G': int(player_row.get('G', 0) or 0),
                    'AB': int(player_row.get('AB', 0) or 0),
                    'PA': int(player_row.get('PA', 0) or 0),
                    'H': int(player_row.get('H', 0) or 0),
                    '2B': int(player_row.get('2B', 0) or 0),
                    '3B': int(player_row.get('3B', 0) or 0),
                    'HR': int(player_row.get('HR', 0) or 0),
                    'R': int(player_row.get('R', 0) or 0),
                    'RBI': int(player_row.get('RBI', 0) or 0),
                    'BB': int(player_row.get('BB', 0) or 0),
                    'SO': int(player_row.get('SO', 0) or 0),
                    'SB': int(player_row.get('SB', 0) or 0),
                    'CS': int(player_row.get('CS', 0) or 0),
                    'HBP': int(player_row.get('HBP', 0) or 0),
                    'AVG': round(float(player_row.get('AVG', 0) or 0), 3),
                    'OBP': round(float(player_row.get('OBP', 0) or 0), 3),
                    'SLG': round(float(player_row.get('SLG', 0) or 0), 3),
                    'OPS': round(float(player_row.get('OPS', 0) or 0), 3),
                }
                stats['1B'] = stats['H'] - stats['2B'] - stats['3B'] - stats['HR']
                
                player['stats'] = stats
                player['fantasy_points'] = calculate_batting_fantasy_points(stats, batting_scoring)
                player['mlb_team'] = player_row.get('Team', '')
                matched_count += 1
            else:
                unmatched_players.append(f"{player_name} (B)")
                
        elif position_type == 'P':
            fg_name = match_player_name(player_name, pitching_names)
            if fg_name and not pitching_df.empty:
                player_row = pitching_df[pitching_df['Name'] == fg_name].iloc[0]
                
                stats = {
                    'G': int(player_row.get('G', 0) or 0),
                    'GS': int(player_row.get('GS', 0) or 0),
                    'W': int(player_row.get('W', 0) or 0),
                    'L': int(player_row.get('L', 0) or 0),
                    'SV': int(player_row.get('SV', 0) or 0),
                    'HLD': int(player_row.get('HLD', 0) or 0) if 'HLD' in player_row else 0,
                    'IP': round(float(player_row.get('IP', 0) or 0), 1),
                    'H': int(player_row.get('H', 0) or 0),
                    'ER': int(player_row.get('ER', 0) or 0),
                    'HR': int(player_row.get('HR', 0) or 0),
                    'BB': int(player_row.get('BB', 0) or 0),
                    'SO': int(player_row.get('SO', 0) or 0),
                    'ERA': round(float(player_row.get('ERA', 0) or 0), 2),
                    'WHIP': round(float(player_row.get('WHIP', 0) or 0), 2),
                    'K/9': round(float(player_row.get('K/9', 0) or 0), 2),
                    'BB/9': round(float(player_row.get('BB/9', 0) or 0), 2),
                }
                
                player['stats'] = stats
                player['fantasy_points'] = calculate_pitching_fantasy_points(stats, pitching_scoring)
                player['mlb_team'] = player_row.get('Team', '')
                matched_count += 1
            else:
                unmatched_players.append(f"{player_name} (P)")
    
    print(f"    ✓ Matched {matched_count}/{len(all_players)} players")
    
    if unmatched_players and len(unmatched_players) <= 20:
        print(f"    Unmatched: {', '.join(unmatched_players)}")
    elif unmatched_players:
        print(f"    {len(unmatched_players)} unmatched players")
    
    all_players.sort(key=lambda x: x.get('fantasy_points', 0), reverse=True)
    return all_players

# =============================================================================
# MANAGER STATS FUNCTIONS
# =============================================================================

def calculate_manager_stats(all_seasons_data: Dict) -> Dict:
    """Calculate all-time manager statistics"""
    manager_stats = {}
    
    for year, data in all_seasons_data.items():
        standings = data.get('standings', [])
        
        if year == 2019:
            standings = correct_2019_playoffs(data)
        
        for team in standings:
            manager_raw = team['manager']
            
            if manager_raw == "Logan":
                if year == 2023:
                    if team['team_key'] == "422.l.6780.t.4":
                        manager = "Logan C"
                    elif team['team_key'] == "422.l.6780.t.12":
                        manager = "Logan S"
                    elif "Draft Pool" in team['team_name']:
                        manager = "Logan C"
                    elif "Peanut Butter" in team['team_name'] or "Elly" in team['team_name']:
                        manager = "Logan S"
                    else:
                        manager = "Logan"
                elif year >= 2020 and year <= 2022:
                    manager = "Logan C"
                elif year >= 2024:
                    manager = "Logan S"
                else:
                    manager = "Logan"
            elif manager_raw == "Josh":
                if year >= 2019 and year <= 2022:
                    if team['team_key'].endswith('t.1'):
                        manager = "Josh B"
                    else:
                        manager = "Josh S"
                elif year >= 2023:
                    if team['team_key'].endswith('t.1'):
                        manager = "Josh B"
                    else:
                        manager = "Josh"
                else:
                    manager = "Josh"
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
            if team['rank'] <= 6:
                manager_stats[manager]['playoff_appearances'] += 1
            
            manager_stats[manager]['season_history'].append({
                'year': year,
                'team_name': team['team_name'],
                'rank': team['rank'],
                'wins': team['wins'],
                'losses': team['losses'],
                'points_for': team['points_for']
            })
    
    for manager in manager_stats.values():
        total_games = manager['total_wins'] + manager['total_losses']
        manager['win_pct'] = round(manager['total_wins'] / total_games, 3) if total_games > 0 else 0
        manager['avg_finish'] = round(
            sum(s['rank'] for s in manager['season_history']) / len(manager['season_history']), 1
        )
    
    return manager_stats

def correct_2019_playoffs(season_data: Dict) -> List[Dict]:
    """Correct 2019 standings based on actual playoff results"""
    standings = season_data.get('standings', [])
    
    CORRECT_2019_RANKS = {'Ryan': 1, 'Rich': 2, 'Tyler': 3}
    
    corrected_standings = []
    for team in standings:
        team_copy = team.copy()
        manager = team['manager']
        
        if manager in CORRECT_2019_RANKS:
            team_copy['rank'] = CORRECT_2019_RANKS[manager]
        
        corrected_standings.append(team_copy)
    
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
        f"{DATA_DIR}/managers",
        f"{DATA_DIR}/players"
    ]
    for directory in dirs:
        os.makedirs(directory, exist_ok=True)

def collect_historical_data(oauth):
    """Collect all historical season data"""
    print("Collecting historical data...")
    
    for year in HISTORICAL_SEASONS:
        print(f"\nCollecting {year} season...")
        
        standings = get_standings(oauth, year)
        scores = get_all_season_scores(oauth, year)
        try:
            draft = get_draft_results(oauth, year)
        except:
            draft = []
        teams = get_teams(oauth, year)
        
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

def collect_historical_player_data(oauth):
    """Collect player stats and transactions for historical seasons"""
    print("\nCollecting historical player data...")
    
    for year in HISTORICAL_SEASONS:
        print(f"\n{'='*50}")
        print(f"Collecting {year} player data...")
        print('='*50)
        
        year_dir = f"{DATA_DIR}/historical/{year}"
        os.makedirs(year_dir, exist_ok=True)
        
        try:
            player_stats = build_player_stats(oauth, year)
            with open(f"{year_dir}/player_stats.json", 'w') as f:
                json.dump(player_stats, f, indent=2)
            print(f"  ✓ Player stats saved ({len(player_stats)} players)")
        except Exception as e:
            print(f"  ⚠ Could not collect player stats: {e}")
            import traceback
            traceback.print_exc()
        
        try:
            transactions = get_all_transactions(oauth, year)
            with open(f"{year_dir}/transactions.json", 'w') as f:
                json.dump(transactions, f, indent=2)
            print(f"  ✓ Transactions saved ({len(transactions)} transactions)")
        except Exception as e:
            print(f"  ⚠ Could not collect transactions: {e}")
        
        try:
            scoring = get_league_scoring_settings(oauth, year)
            with open(f"{year_dir}/scoring_settings.json", 'w') as f:
                json.dump(scoring, f, indent=2)
            print(f"  ✓ Scoring settings saved")
        except Exception as e:
            print(f"  ⚠ Could not save scoring settings: {e}")

def collect_current_season_data(oauth):
    """Collect current season data"""
    print(f"\nUpdating {CURRENT_SEASON} season data...")
    
    current_season_dir = f"{DATA_DIR}/current_season"
    os.makedirs(current_season_dir, exist_ok=True)
    
    standings = get_standings(oauth, CURRENT_SEASON)
    teams = get_teams(oauth, CURRENT_SEASON)
    scores = get_all_season_scores(oauth, CURRENT_SEASON, num_weeks=26)
    
    with open(f"{current_season_dir}/standings.json", 'w') as f:
        json.dump(standings, f, indent=2)
    with open(f"{current_season_dir}/teams.json", 'w') as f:
        json.dump(teams, f, indent=2)
    with open(f"{current_season_dir}/all_scores.json", 'w') as f:
        json.dump(scores, f, indent=2)
    
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

def collect_current_season_player_data(oauth):
    """Collect player stats and transactions for current season"""
    print(f"\n{'='*50}")
    print(f"Updating {CURRENT_SEASON} player data...")
    print('='*50)
    
    current_season_dir = f"{DATA_DIR}/current_season"
    os.makedirs(current_season_dir, exist_ok=True)
    
    try:
        player_stats = build_player_stats(oauth, CURRENT_SEASON)
        with open(f"{current_season_dir}/player_stats.json", 'w') as f:
            json.dump(player_stats, f, indent=2)
        print(f"  ✓ Player stats saved ({len(player_stats)} players)")
    except Exception as e:
        print(f"  ⚠ Could not collect player stats: {e}")
        import traceback
        traceback.print_exc()
    
    try:
        transactions = get_all_transactions(oauth, CURRENT_SEASON)
        with open(f"{current_season_dir}/transactions.json", 'w') as f:
            json.dump(transactions, f, indent=2)
        print(f"  ✓ Transactions saved ({len(transactions)} transactions)")
    except Exception as e:
        print(f"  ⚠ Could not collect transactions: {e}")
    
    try:
        scoring = get_league_scoring_settings(oauth, CURRENT_SEASON)
        with open(f"{current_season_dir}/scoring_settings.json", 'w') as f:
            json.dump(scoring, f, indent=2)
        print(f"  ✓ Scoring settings saved")
    except Exception as e:
        print(f"  ⚠ Could not save scoring settings: {e}")

def update_manager_stats(oauth):
    """Update manager statistics"""
    print("\nUpdating manager statistics...")
    
    all_seasons = {}
    
    for year in HISTORICAL_SEASONS:
        standings_file = f"{DATA_DIR}/historical/{year}/final_standings.json"
        scores_file = f"{DATA_DIR}/historical/{year}/all_scores.json"
        
        if os.path.exists(standings_file):
            with open(standings_file, 'r') as f:
                standings = json.load(f)
            scores = []
            if os.path.exists(scores_file):
                with open(scores_file, 'r') as f:
                    scores = json.load(f)
            all_seasons[year] = {'standings': standings, 'scores': scores}
            print(f"  ✓ Loaded {year} season")
    
    current_standings_file = f"{DATA_DIR}/current_season/standings.json"
    if os.path.exists(current_standings_file):
        with open(current_standings_file, 'r') as f:
            standings = json.load(f)
        scores = []
        current_scores_file = f"{DATA_DIR}/current_season/all_scores.json"
        if os.path.exists(current_scores_file):
            with open(current_scores_file, 'r') as f:
                scores = json.load(f)
        all_seasons[CURRENT_SEASON] = {'standings': standings, 'scores': scores}
        print(f"  ✓ Loaded {CURRENT_SEASON} season")
    
    if not all_seasons:
        print("  ⚠ No season data available")
        return
    
    manager_stats = calculate_manager_stats(all_seasons)
    
    managers_dir = f"{DATA_DIR}/managers"
    os.makedirs(managers_dir, exist_ok=True)
    
    with open(f"{managers_dir}/all_time_stats.json", 'w') as f:
        json.dump(list(manager_stats.values()), f, indent=2)
    
    manager_history = []
    for manager_data in manager_stats.values():
        for season in manager_data['season_history']:
            manager_history.append({'manager': manager_data['manager_name'], **season})
    
    with open(f"{managers_dir}/manager_history.json", 'w') as f:
        json.dump(manager_history, f, indent=2)
    
    print(f"✓ Manager stats updated ({len(manager_stats)} managers)")

def build_player_career_history():
    """Aggregate player data across all seasons"""
    print("\nBuilding player career history...")
    
    player_careers = {}
    
    for year in HISTORICAL_SEASONS + [CURRENT_SEASON]:
        if year == CURRENT_SEASON:
            player_file = f"{DATA_DIR}/current_season/player_stats.json"
        else:
            player_file = f"{DATA_DIR}/historical/{year}/player_stats.json"
        
        if os.path.exists(player_file):
            with open(player_file, 'r') as f:
                players = json.load(f)
            
            for player in players:
                name = player.get('name', '')
                if not name:
                    continue
                
                name_key = normalize_player_name(name)
                
                if name_key not in player_careers:
                    player_careers[name_key] = {
                        'name': name,
                        'seasons': [],
                        'career_fantasy_points': 0
                    }
                
                player_careers[name_key]['seasons'].append({
                    'year': year,
                    'team_name': player.get('team_name', ''),
                    'manager': player.get('manager', ''),
                    'fantasy_points': player.get('fantasy_points', 0),
                    'position_type': player.get('position_type', ''),
                    'stats': player.get('stats', {})
                })
                player_careers[name_key]['career_fantasy_points'] += player.get('fantasy_points', 0)
    
    players_dir = f"{DATA_DIR}/players"
    os.makedirs(players_dir, exist_ok=True)
    
    with open(f"{players_dir}/player_history.json", 'w') as f:
        json.dump(player_careers, f, indent=2)
    
    print(f"✓ Player career history built ({len(player_careers)} players)")

def create_league_info():
    """Create league metadata file"""
    league_info = {
        'league_name': 'Fantasy Baseball Civil War',
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
    
    if HISTORICAL_SEASONS:
        collect_historical_data(oauth)
    
    collect_current_season_data(oauth)
    update_manager_stats(oauth)
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
    collect_current_season_data(oauth)
    update_manager_stats(oauth)
    create_league_info()
    
    print("\n" + "=" * 60)
    print("✓ Weekly update complete!")
    print("=" * 60)

def player_data_setup():
    """Run this to collect player stats and transactions for all seasons"""
    print("=" * 60)
    print("PLAYER DATA SETUP - Collecting Player Stats & Transactions")
    print("(Using Fangraphs for stats, Yahoo for rosters)")
    print("=" * 60)
    
    if not PYBASEBALL_AVAILABLE:
        print("\n⚠ ERROR: pybaseball is required for player stats.")
        print("Install it with: pip install pybaseball")
        return
    
    create_directory_structure()
    oauth = setup_oauth()
    
    if HISTORICAL_SEASONS:
        collect_historical_player_data(oauth)
    
    collect_current_season_player_data(oauth)
    build_player_career_history()
    
    print("\n" + "=" * 60)
    print("✓ Player data setup complete!")
    print("=" * 60)

def weekly_update_with_players():
    """Run this WEEKLY to update everything including player data"""
    print("=" * 60)
    print(f"FULL WEEKLY UPDATE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    oauth = setup_oauth()
    collect_current_season_data(oauth)
    
    if PYBASEBALL_AVAILABLE:
        collect_current_season_player_data(oauth)
        build_player_career_history()
    else:
        print("\n⚠ pybaseball not available, skipping player stats")
    
    update_manager_stats(oauth)
    create_league_info()
    
    print("\n" + "=" * 60)
    print("✓ Full weekly update complete!")
    print("=" * 60)

def check_available_seasons():
    """Check which seasons your Yahoo account has league data for"""
    print("=" * 60)
    print("CHECKING AVAILABLE SEASONS")
    print("=" * 60)
    
    oauth = setup_oauth()
    gm = Game(oauth, 'mlb')
    
    for year in range(2015, 2026):
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
                    except:
                        print(f"  - ID: {league_id}")
        except:
            pass
    
    print("\n" + "=" * 60)

def test_fangraphs():
    """Test Fangraphs data fetching"""
    print("=" * 60)
    print("TESTING FANGRAPHS DATA")
    print("=" * 60)
    
    if not PYBASEBALL_AVAILABLE:
        print("\n⚠ pybaseball not installed!")
        print("Install with: pip install pybaseball")
        return
    
    print("\nNote: First-time fetches can take 1-2 minutes as data is downloaded.")
    print("Subsequent runs use cached data and are much faster.\n")
    
    print("Fetching 2024 batting stats...")
    batters = get_fangraphs_batting_stats(2024)
    if not batters.empty:
        print(f"✓ Got {len(batters)} batters")
        print(batters.head(3)[['Name', 'Team', 'G', 'HR', 'RBI']].to_string())
    else:
        print("✗ Failed to get batting stats")
    
    print("\nFetching 2024 pitching stats...")
    pitchers = get_fangraphs_pitching_stats(2024)
    if not pitchers.empty:
        print(f"✓ Got {len(pitchers)} pitchers")
        print(pitchers.head(3)[['Name', 'Team', 'W', 'SO', 'ERA']].to_string())
    else:
        print("✗ Failed to get pitching stats")
    
    print("\n" + "=" * 60)
    if not batters.empty and not pitchers.empty:
        print("✓ Fangraphs test complete! You can now run 'python collect_data.py players'")
    else:
        print("⚠ Fangraphs test had issues. Check your internet connection.")
    print("=" * 60)

def test_single_year_players(year: int = 2024):
    """Test player data collection for a single year"""
    print("=" * 60)
    print(f"TESTING PLAYER DATA COLLECTION FOR {year}")
    print("=" * 60)
    
    if not PYBASEBALL_AVAILABLE:
        print("\n⚠ pybaseball not installed!")
        print("Install with: pip install pybaseball")
        return
    
    create_directory_structure()
    oauth = setup_oauth()
    
    print(f"\nCollecting {year} player data...")
    player_stats = build_player_stats(oauth, year)
    
    if player_stats:
        # Save to test file
        test_file = f"{DATA_DIR}/test_player_stats_{year}.json"
        with open(test_file, 'w') as f:
            json.dump(player_stats, f, indent=2)
        
        print(f"\n✓ Collected {len(player_stats)} players")
        print(f"✓ Saved to {test_file}")
        
        # Show top 5 by fantasy points
        print(f"\nTop 5 players by fantasy points:")
        for i, p in enumerate(player_stats[:5]):
            print(f"  {i+1}. {p['name']} ({p['manager']}) - {p['fantasy_points']} pts")
    else:
        print("✗ Failed to collect player data")
    
    print("\n" + "=" * 60)

# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "setup":
            initial_setup()
        elif command == "check":
            check_available_seasons()
        elif command == "players":
            player_data_setup()
        elif command == "full":
            weekly_update_with_players()
        elif command == "test-fangraphs":
            test_fangraphs()
        elif command == "test-year":
            # Test single year - defaults to 2024, or specify year as 3rd arg
            year = int(sys.argv[2]) if len(sys.argv) > 2 else 2024
            test_single_year_players(year)
        else:
            print(f"Unknown command: {command}")
            print("\nAvailable commands:")
            print("  setup          - Initial setup (historical data)")
            print("  check          - Check available seasons")
            print("  players        - Collect player stats (Fangraphs + Yahoo)")
            print("  full           - Weekly update with player data")
            print("  test-fangraphs - Test Fangraphs connection")
            print("  test-year [yr] - Test player collection for one year (default 2024)")
            print("  (none)         - Weekly update (no player data)")
    else:
        weekly_update()
