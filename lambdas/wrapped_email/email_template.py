"""
XOMIFY Email Template Generator
================================
Generates beautiful HTML emails with Xomify's purple/green branding.
"""

from typing import List


def generate_email_html(
    month_name: str,
    top_songs: List[str],
    top_artists: List[str],
    top_genres: List[str],
    xomify_url: str,
    unsubscribe_url: str
) -> str:
    """
    Generate the HTML email for monthly wrapped preview.
    
    Args:
        month_name: Display name like "December 2024"
        top_songs: List of top 5 song names with artists
        top_artists: List of top 5 artist names
        top_genres: List of top 5 genre names
        xomify_url: Base URL for Xomify
        unsubscribe_url: Unsubscribe link
    
    Returns:
        HTML string for the email
    """
    
    # Generate list items for songs
    songs_html = ""
    for i, song in enumerate(top_songs):
        songs_html += f'''
        <tr>
            <td style="padding: 12px 16px; border-bottom: 1px solid rgba(255,255,255,0.06);">
                <table cellpadding="0" cellspacing="0" border="0" width="100%">
                    <tr>
                        <td width="32" style="color: #1bdc6f; font-weight: 700; font-size: 14px;">#{i+1}</td>
                        <td style="color: #ffffff; font-size: 15px; font-weight: 500;">{song}</td>
                    </tr>
                </table>
            </td>
        </tr>
        '''
    
    # Generate list items for artists
    artists_html = ""
    for i, artist in enumerate(top_artists):
        artists_html += f'''
        <tr>
            <td style="padding: 12px 16px; border-bottom: 1px solid rgba(255,255,255,0.06);">
                <table cellpadding="0" cellspacing="0" border="0" width="100%">
                    <tr>
                        <td width="32" style="color: #1bdc6f; font-weight: 700; font-size: 14px;">#{i+1}</td>
                        <td style="color: #ffffff; font-size: 15px; font-weight: 500;">{artist}</td>
                    </tr>
                </table>
            </td>
        </tr>
        '''
    
    # Generate genre pills
    genres_html = ""
    for genre in top_genres:
        genres_html += f'''
        <span style="display: inline-block; background: linear-gradient(135deg, rgba(156,10,191,0.3) 0%, rgba(156,10,191,0.15) 100%); 
                     color: #c77ddb; padding: 6px 14px; border-radius: 20px; font-size: 13px; font-weight: 500; 
                     margin: 4px; border: 1px solid rgba(156,10,191,0.3);">{genre}</span>
        '''
    
    html = f'''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <title>Your {month_name} Wrapped is Ready!</title>
</head>
<body style="margin: 0; padding: 0; background-color: #0a0a14; font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, 'Roboto', 'Helvetica Neue', Arial, sans-serif;">
    
    <!-- Wrapper Table -->
    <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background-color: #0a0a14;">
        <tr>
            <td align="center" style="padding: 40px 20px;">
                
                <!-- Main Container -->
                <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="600" style="max-width: 600px; width: 100%;">
                    
                    <!-- Header with Logo -->
                    <tr>
                        <td align="center" style="padding-bottom: 32px;">
                            <img src="{xomify_url}/assets/img/banner-logo-x-rework.png" alt="XOMIFY" width="150" style="display: block; max-width: 150px; height: auto;">
                        </td>
                    </tr>
                    
                    <!-- Hero Section -->
                    <tr>
                        <td style="background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%); border-radius: 24px 24px 0 0; padding: 40px 32px; text-align: center;">
                            <h1 style="margin: 0 0 8px 0; font-size: 28px; font-weight: 800; color: #ffffff;">
                                Your {month_name} Wrapped
                            </h1>
                            <p style="margin: 0; color: #8a8a9a; font-size: 16px;">
                                Here's a sneak peek at your listening highlights
                            </p>
                        </td>
                    </tr>
                    
                    <!-- Stats Preview Bar -->
                    <tr>
                        <td style="background: linear-gradient(135deg, #9c0abf 0%, #1bdc6f 100%); padding: 3px;">
                            <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background: #121225;">
                                <tr>
                                    <td style="padding: 16px 32px;">
                                        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
                                            <tr>
                                                <td width="33%" align="center" style="color: #ffffff;">
                                                    <div style="font-size: 24px; font-weight: 700; color: #1bdc6f;">{len(top_songs)}</div>
                                                    <div style="font-size: 11px; color: #8a8a9a; text-transform: uppercase; letter-spacing: 0.5px;">Top Songs</div>
                                                </td>
                                                <td width="33%" align="center" style="color: #ffffff;">
                                                    <div style="font-size: 24px; font-weight: 700; color: #9c0abf;">{len(top_artists)}</div>
                                                    <div style="font-size: 11px; color: #8a8a9a; text-transform: uppercase; letter-spacing: 0.5px;">Top Artists</div>
                                                </td>
                                                <td width="33%" align="center" style="color: #ffffff;">
                                                    <div style="font-size: 24px; font-weight: 700; color: #1bdc6f;">{len(top_genres)}</div>
                                                    <div style="font-size: 11px; color: #8a8a9a; text-transform: uppercase; letter-spacing: 0.5px;">Top Genres</div>
                                                </td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                    
                    <!-- Content Section -->
                    <tr>
                        <td style="background: linear-gradient(180deg, #121225 0%, #0a0a14 100%); padding: 32px;">
                            
                            <!-- Top Songs -->
                            <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-bottom: 28px;">
                                <tr>
                                    <td style="padding-bottom: 16px;">
                                        <table cellpadding="0" cellspacing="0" border="0">
                                            <tr>
                                                <td style="background: linear-gradient(135deg, rgba(156,10,191,0.2) 0%, rgba(156,10,191,0.1) 100%); 
                                                           border-radius: 8px; padding: 8px 12px;">
                                                    <span style="color: #c77ddb; font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 1px;">
                                                        🎵 Top Songs
                                                    </span>
                                                </td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                                <tr>
                                    <td style="background: rgba(255,255,255,0.03); border-radius: 12px; border: 1px solid rgba(255,255,255,0.06);">
                                        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
                                            {songs_html}
                                        </table>
                                    </td>
                                </tr>
                            </table>
                            
                            <!-- Top Artists -->
                            <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-bottom: 28px;">
                                <tr>
                                    <td style="padding-bottom: 16px;">
                                        <table cellpadding="0" cellspacing="0" border="0">
                                            <tr>
                                                <td style="background: linear-gradient(135deg, rgba(27,220,111,0.2) 0%, rgba(27,220,111,0.1) 100%); 
                                                           border-radius: 8px; padding: 8px 12px;">
                                                    <span style="color: #1bdc6f; font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 1px;">
                                                        🎤 Top Artists
                                                    </span>
                                                </td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                                <tr>
                                    <td style="background: rgba(255,255,255,0.03); border-radius: 12px; border: 1px solid rgba(255,255,255,0.06);">
                                        <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
                                            {artists_html}
                                        </table>
                                    </td>
                                </tr>
                            </table>
                            
                            <!-- Top Genres -->
                            <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="margin-bottom: 32px;">
                                <tr>
                                    <td style="padding-bottom: 16px;">
                                        <table cellpadding="0" cellspacing="0" border="0">
                                            <tr>
                                                <td style="background: linear-gradient(135deg, rgba(59,130,246,0.2) 0%, rgba(59,130,246,0.1) 100%); 
                                                           border-radius: 8px; padding: 8px 12px;">
                                                    <span style="color: #60a5fa; font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 1px;">
                                                        🎧 Top Genres
                                                    </span>
                                                </td>
                                            </tr>
                                        </table>
                                    </td>
                                </tr>
                                <tr>
                                    <td align="center" style="padding: 8px 0;">
                                        {genres_html}
                                    </td>
                                </tr>
                            </table>
                            
                            <!-- CTA Button -->
                            <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
                                <tr>
                                    <td align="center">
                                        <a href="{xomify_url}/wrapped" 
                                           style="display: inline-block; background: linear-gradient(135deg, #9c0abf 0%, #7a0896 100%); 
                                                  color: #ffffff; text-decoration: none; padding: 16px 40px; border-radius: 30px; 
                                                  font-size: 16px; font-weight: 600;">
                                            View Your Full Wrapped →
                                        </a>
                                    </td>
                                </tr>
                            </table>
                            
                        </td>
                    </tr>
                    
                    <!-- Footer -->
                    <tr>
                        <td style="background: #0a0a14; border-radius: 0 0 24px 24px; padding: 32px; text-align: center; border-top: 1px solid rgba(255,255,255,0.06);">
                            <p style="margin: 0 0 16px 0; color: #6a6a7a; font-size: 13px;">
                                You're receiving this because you're enrolled in Xomify Monthly Wrapped.
                            </p>
                            <p style="margin: 0; color: #6a6a7a; font-size: 13px;">
                                <a href="{unsubscribe_url}" style="color: #9c0abf; text-decoration: underline;">Unsubscribe</a>
                                &nbsp;•&nbsp;
                                <a href="{xomify_url}" style="color: #9c0abf; text-decoration: underline;">Visit Xomify</a>
                            </p>
                            <p style="margin: 16px 0 0 0; color: #4a4a5a; font-size: 12px;">
                                Built with 💜 by @domgiordano
                            </p>
                        </td>
                    </tr>
                    
                </table>
                
            </td>
        </tr>
    </table>
    
</body>
</html>
    '''
    
    return html
