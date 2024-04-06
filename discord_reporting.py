import os
import discord
from datetime import datetime, timedelta, timezone
import pandas as pd

# File path for the CSV. This is where the report will be saved.
csv_file_path = 'Customer_Support_Discord_Reporting.csv'

# Mapping of Discord usernames to real names. Replace these with generic placeholders.
USER_MAPPING = {
    'username1': 'Real Name 1',
    'username2': 'Real Name 2',
    # Add more mappings as needed
}

# Use environment variables for sensitive information
TOKEN = os.getenv('DISCORD_BOT_TOKEN')  # Set your bot's token as an environment variable
SERVER_ID = 123456789012345678  # Replace with your server's ID
CHANNEL_IDS = [123456789012345678, 987654321098765432]  # Replace with your forum channel IDs

# Setting up Discord client with required permissions
intents = discord.Intents.default()
intents.messages = True
intents.reactions = True
intents.members = True
client = discord.Client(intents=intents)

def get_week_range(date):
    """Calculate and return the week range given a date."""
    start_of_week = date - timedelta(days=date.weekday())
    end_of_week = start_of_week + timedelta(days=6)
    return start_of_week.strftime("%m/%d/%Y") + " - " + end_of_week.strftime("%m/%d/%Y")

def determine_time_range(date, current_date):
    """Determine the time range in which a given date falls, relative to the current date."""
    number_of_weeks_to_include = 4
    for i in range(number_of_weeks_to_include):
        start_of_week = current_date - timedelta(days=current_date.weekday() + 7 * i)
        end_of_week = start_of_week + timedelta(days=6)
        if start_of_week <= date <= end_of_week:
            return get_week_range(start_of_week)
    return "Older than " + get_week_range(current_date - timedelta(days=current_date.weekday() + 7 * number_of_weeks_to_include))

async def fetch_threads(forum_channel):
    """Fetch and return all threads from a given forum channel."""
    thread_data = []
    for thread in forum_channel.threads:
        thread_creation_date = thread.created_at.date()
        thread_data.append((thread, thread_creation_date))
    return thread_data

async def fetch_messages_from_thread(thread):
    """Fetch and return all messages from a given thread that were posted in the last 4 weeks."""
    after_date = datetime.now(timezone.utc) - timedelta(weeks=4)
    messages = []
    thread_creation_date = None
    thread_title = thread.name
    async for message in thread.history(limit=1, oldest_first=True):
        thread_creation_date = message.created_at
        break
    async for message in thread.history(limit=None, after=after_date):
        messages.append((message, thread_creation_date, thread_title))
    return messages

@client.event
async def on_ready():
    """Event handler for when the Discord client is ready."""
    print(f'Logged in as {client.user}')
    guild = client.get_guild(SERVER_ID)
    if not guild:
        print(f"Guild with ID {SERVER_ID} not found.")
        return
    print(f"Listing all forum channels in the guild '{guild.name}':")

    current_date = datetime.now(timezone.utc).date()
    weekly_totals = {}  # Weekly totals including thread count
    user_weekly_data = {}  # Data per user per week

    for channel_id in CHANNEL_IDS:
        channel = guild.get_channel(channel_id)
        if channel is None or not isinstance(channel, discord.ForumChannel):
            continue

        try:
            threads = await fetch_threads(channel)
            for thread, creation_date in threads:
                time_range = determine_time_range(creation_date, current_date)
                if time_range not in weekly_totals:
                    weekly_totals[time_range] = {'Total Messages Sent': 0, 'Number of Questions Replied to': set(), 'Total Questions Asked': 0}
                weekly_totals[time_range]['Total Questions Asked'] += 1
                
                messages = await fetch_messages_from_thread(thread)
                for message, thread_creation_date, _ in messages:
                    user = message.author.name
                    if user in USER_MAPPING:
                        time_range = determine_time_range(thread_creation_date.date(), current_date)
                        if user not in user_weekly_data:
                            user_weekly_data[user] = {}
                        if time_range not in user_weekly_data[user]:
                            user_weekly_data[user][time_range] = {'Total Messages Sent': 0, 'Number of Questions Replied to': set()}
                        user_weekly_data[user][time_range]['Total Messages Sent'] += 1
                        user_weekly_data[user][time_range]['Number of Questions Replied to'].add(thread.id)
                        weekly_totals[time_range]['Total Messages Sent'] += 1
                        weekly_totals[time_range]['Number of Questions Replied to'].add(thread.id)
        except discord.Forbidden:
            print(f"Skipping forum channel {channel.name}, lack permissions.")

    final_data = []
    for user, data in user_weekly_data.items():
        for time_range, stats in data.items():
            final_data.append({
                'Time Range': time_range,
                'User': USER_MAPPING.get(user, user),
                'Number of Questions Replied to': len(stats['Number of Questions Replied to']),
                'Total Number of Messages Sent': stats['Total Messages Sent'],
                'Total Questions Asked': ''
            })

    # Append weekly totals for all users, including total question counts
    for time_range, totals in weekly_totals.items():
        final_data.append({
            'Time Range': time_range,
            'User': 'All Users',
            'Number of Questions Replied to': len(totals['Number of Questions Replied to']),
            'Total Number of Messages Sent': totals['Total Messages Sent'],
            'Total Questions Asked': totals['Total Questions Asked']
        })

    df = pd.DataFrame(final_data)
    df.to_csv(csv_file_path, index=False)

    print("Data collection and processing complete.")

    await client.close()

client.run(TOKEN)
