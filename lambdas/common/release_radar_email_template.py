"""
XOMIFY Release Radar Email Template
===================================
HTML email template for weekly release radar notifications.
"""

from datetime import datetime
import random


def generate_release_radar_email(
    user_name: str,
    week_key: str,
    stats: dict,
    releases: list,
    playlist_url: str,
    preview_count: int = 3
) -> str:
    """
    Generate HTML email for weekly release radar.
    
    Args:
        user_name: User's display name
        week_key: Week key in "YYYY-WW" format
        stats: Dict with totalTracks, albumCount, singleCount, appearsOnCount
        releases: List of release objects
        playlist_url: URL to the Spotify playlist
        preview_count: Number of releases to preview (default 3)
        
    Returns:
        HTML email string
    """
    
    # Get random preview releases
    previews = get_random_previews(releases, preview_count)
    
    # Format week display
    week_display = format_week_display(week_key)
    
    # Build preview HTML
    preview_html = build_preview_section(previews)
    
    # Build stats section
    stats_html = build_stats_section(stats)
    
    return f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Your Weekly Release Radar</title>
</head>
<body style="margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; background-color: #0a0a14;">
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background-color: #0a0a14;">
        <tr>
            <td align="center" style="padding: 40px 20px;">
                <table role="presentation" width="600" cellspacing="0" cellpadding="0" style="max-width: 600px; width: 100%;">
                    
                    <!-- Header -->
                    <tr>
                        <td align="center" style="padding-bottom: 30px;">
                            <img src="https://xomify.com/assets/logo.png" alt="Xomify" width="120" style="display: block;">
                        </td>
                    </tr>
                    
                    <!-- Main Card -->
                    <tr>
                        <td style="background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%); border-radius: 20px; padding: 40px 30px;">
                            
                            <!-- Title -->
                            <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
                                <tr>
                                    <td align="center" style="padding-bottom: 10px;">
                                        <span style="font-size: 32px;">📻</span>
                                    </td>
                                </tr>
                                <tr>
                                    <td align="center" style="padding-bottom: 8px;">
                                        <h1 style="margin: 0; font-size: 28px; font-weight: 700; color: #ffffff;">
                                            Your Weekly Release Radar
                                        </h1>
                                    </td>
                                </tr>
                                <tr>
                                    <td align="center" style="padding-bottom: 30px;">
                                        <p style="margin: 0; font-size: 14px; color: #8a8a9a;">
                                            {week_display}
                                        </p>
                                    </td>
                                </tr>
                            </table>
                            
                            <!-- Greeting -->
                            <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
                                <tr>
                                    <td style="padding-bottom: 25px;">
                                        <p style="margin: 0; font-size: 16px; color: #ffffff; line-height: 1.6;">
                                            Hey {user_name}! 👋
                                        </p>
                                        <p style="margin: 10px 0 0 0; font-size: 16px; color: #b0b0c0; line-height: 1.6;">
                                            Artists you follow dropped <strong style="color: #1bdc6f;">{stats.get('totalTracks', 0)} new tracks</strong> this week!
                                        </p>
                                    </td>
                                </tr>
                            </table>
                            
                            <!-- Stats Section -->
                            {stats_html}
                            
                            <!-- Divider -->
                            <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
                                <tr>
                                    <td style="padding: 25px 0;">
                                        <div style="height: 1px; background: linear-gradient(90deg, transparent, rgba(156, 10, 191, 0.3), transparent);"></div>
                                    </td>
                                </tr>
                            </table>
                            
                            <!-- Preview Section -->
                            <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
                                <tr>
                                    <td style="padding-bottom: 20px;">
                                        <h2 style="margin: 0; font-size: 18px; font-weight: 600; color: #ffffff;">
                                            🎵 Highlights
                                        </h2>
                                    </td>
                                </tr>
                            </table>
                            
                            {preview_html}
                            
                            <!-- CTA Button -->
                            <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
                                <tr>
                                    <td align="center" style="padding-top: 30px;">
                                        <a href="{playlist_url}" 
                                           style="display: inline-block; 
                                                  background: linear-gradient(135deg, #1db954 0%, #1ed760 100%); 
                                                  color: #ffffff; 
                                                  text-decoration: none; 
                                                  padding: 16px 40px; 
                                                  border-radius: 30px; 
                                                  font-size: 16px; 
                                                  font-weight: 600;
                                                  box-shadow: 0 4px 15px rgba(29, 185, 84, 0.3);">
                                            Open Full Playlist →
                                        </a>
                                    </td>
                                </tr>
                            </table>
                            
                        </td>
                    </tr>
                    
                    <!-- Footer -->
                    <tr>
                        <td align="center" style="padding: 30px 20px;">
                            <p style="margin: 0 0 10px 0; font-size: 12px; color: #6a6a7a;">
                                You're receiving this because you enrolled in Release Radar on Xomify.
                            </p>
                            <p style="margin: 0; font-size: 12px; color: #6a6a7a;">
                                <a href="https://xomify.com/release-radar" style="color: #9c0abf; text-decoration: none;">Manage preferences</a>
                                &nbsp;•&nbsp;
                                <a href="https://xomify.com" style="color: #9c0abf; text-decoration: none;">Visit Xomify</a>
                            </p>
                        </td>
                    </tr>
                    
                </table>
            </td>
        </tr>
    </table>
</body>
</html>
"""


def get_random_previews(releases: list, count: int) -> list:
    """Get random preview releases."""
    if not releases:
        return []
    
    # Prefer albums over singles for previews
    albums = [r for r in releases if r.get('albumType') == 'album']
    singles = [r for r in releases if r.get('albumType') == 'single']
    others = [r for r in releases if r.get('albumType') not in ['album', 'single']]
    
    previews = []
    
    # Try to get at least one album if available
    if albums:
        previews.append(random.choice(albums))
        albums.remove(previews[0])
    
    # Fill rest randomly
    remaining = albums + singles + others
    random.shuffle(remaining)
    
    while len(previews) < count and remaining:
        previews.append(remaining.pop(0))
    
    return previews


def format_week_display(week_key: str) -> str:
    """Format week key for display."""
    try:
        year, week = map(int, week_key.split('-'))
        # Get approximate date range
        jan_4 = datetime(year, 1, 4)
        start_of_week_1 = jan_4 - datetime.timedelta(days=jan_4.weekday())
        monday = start_of_week_1 + datetime.timedelta(weeks=week - 1)
        saturday = monday - datetime.timedelta(days=2)
        friday = saturday + datetime.timedelta(days=6)
        
        if saturday.month == friday.month:
            return f"{saturday.strftime('%B %d')} - {friday.strftime('%d, %Y')}"
        else:
            return f"{saturday.strftime('%B %d')} - {friday.strftime('%B %d, %Y')}"
    except:
        return f"Week {week_key}"


def build_stats_section(stats: dict) -> str:
    """Build the stats HTML section."""
    album_count = stats.get('albumCount', 0)
    single_count = stats.get('singleCount', 0)
    appears_on_count = stats.get('appearsOnCount', 0)
    
    return f"""
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
        <tr>
            <td>
                <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background: rgba(255, 255, 255, 0.03); border-radius: 12px; padding: 20px;">
                    <tr>
                        <td width="33%" align="center" style="padding: 10px;">
                            <p style="margin: 0; font-size: 24px; font-weight: 700; color: #9c0abf;">{album_count}</p>
                            <p style="margin: 5px 0 0 0; font-size: 12px; color: #8a8a9a; text-transform: uppercase; letter-spacing: 0.5px;">Albums</p>
                        </td>
                        <td width="33%" align="center" style="padding: 10px; border-left: 1px solid rgba(255,255,255,0.1); border-right: 1px solid rgba(255,255,255,0.1);">
                            <p style="margin: 0; font-size: 24px; font-weight: 700; color: #1bdc6f;">{single_count}</p>
                            <p style="margin: 5px 0 0 0; font-size: 12px; color: #8a8a9a; text-transform: uppercase; letter-spacing: 0.5px;">Singles</p>
                        </td>
                        <td width="33%" align="center" style="padding: 10px;">
                            <p style="margin: 0; font-size: 24px; font-weight: 700; color: #ffffff;">{appears_on_count}</p>
                            <p style="margin: 5px 0 0 0; font-size: 12px; color: #8a8a9a; text-transform: uppercase; letter-spacing: 0.5px;">Features</p>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
    </table>
    """


def build_preview_section(previews: list) -> str:
    """Build the preview releases HTML section."""
    if not previews:
        return """
        <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
            <tr>
                <td align="center" style="padding: 20px;">
                    <p style="margin: 0; font-size: 14px; color: #8a8a9a;">
                        No releases to preview this week.
                    </p>
                </td>
            </tr>
        </table>
        """
    
    preview_items = ""
    for release in previews:
        image_url = release.get('imageUrl') or 'https://xomify.com/assets/default-album.png'
        name = release.get('name', 'Unknown Release')[:40]
        artist = release.get('artistName', 'Unknown Artist')[:30]
        release_type = release.get('albumType', 'release').title()
        track_count = release.get('totalTracks', 1)
        
        # Type badge color
        badge_bg = 'rgba(156, 10, 191, 0.2)' if release_type == 'Album' else 'rgba(27, 220, 111, 0.2)'
        badge_color = '#c77ddb' if release_type == 'Album' else '#1bdc6f'
        
        preview_items += f"""
        <tr>
            <td style="padding: 10px 0;">
                <table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background: rgba(255, 255, 255, 0.02); border-radius: 12px;">
                    <tr>
                        <td width="70" style="padding: 12px;">
                            <img src="{image_url}" 
                                 alt="{name}" 
                                 width="56" 
                                 height="56" 
                                 style="display: block; border-radius: 8px; object-fit: cover;">
                        </td>
                        <td style="padding: 12px 12px 12px 0;">
                            <p style="margin: 0; font-size: 15px; font-weight: 600; color: #ffffff; line-height: 1.3;">
                                {name}
                            </p>
                            <p style="margin: 4px 0 8px 0; font-size: 13px; color: #8a8a9a;">
                                {artist}
                            </p>
                            <span style="display: inline-block; 
                                         background: {badge_bg}; 
                                         color: {badge_color}; 
                                         font-size: 10px; 
                                         font-weight: 600; 
                                         text-transform: uppercase; 
                                         padding: 3px 8px; 
                                         border-radius: 10px;
                                         letter-spacing: 0.3px;">
                                {release_type} • {track_count} {'track' if track_count == 1 else 'tracks'}
                            </span>
                        </td>
                    </tr>
                </table>
            </td>
        </tr>
        """
    
    return f"""
    <table role="presentation" width="100%" cellspacing="0" cellpadding="0">
        {preview_items}
    </table>
    """


def generate_release_radar_email_plain_text(
    user_name: str,
    week_key: str,
    stats: dict,
    releases: list,
    playlist_url: str
) -> str:
    """
    Generate plain text version of the release radar email.
    """
    total = stats.get('totalTracks', 0)
    albums = stats.get('albumCount', 0)
    singles = stats.get('singleCount', 0)
    features = stats.get('appearsOnCount', 0)
    
    # Get a few preview names
    preview_names = [r.get('name', 'Unknown') for r in releases[:5]]
    previews_text = '\n'.join([f"  • {name}" for name in preview_names])
    
    return f"""
📻 Your Weekly Release Radar
{'-' * 40}

Hey {user_name}!

Artists you follow dropped {total} new tracks this week!

BREAKDOWN:
  • {albums} Albums
  • {singles} Singles  
  • {features} Features/Collabs

HIGHLIGHTS:
{previews_text}

View your full playlist:
{playlist_url}

---
You're receiving this because you enrolled in Release Radar on Xomify.
Manage preferences: https://xomify.com/release-radar
"""
