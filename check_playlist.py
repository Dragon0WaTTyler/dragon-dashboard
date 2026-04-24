import requests

API_KEY = "AIzaSyDIfVrYDMyMucFImqYYUvm3EXyyIZC7DD8"
PLAYLIST_ID = "PLA9RaIVS6nz25rdZd3SihId_AsAA06nPP"  # <-- your current ID

# 1. Get playlist details (title, total item count)
url = "https://www.googleapis.com/youtube/v3/playlists"
params = {
    "part": "snippet,contentDetails",
    "id": PLAYLIST_ID,
    "key": API_KEY
}
r = requests.get(url, params=params)
data = r.json()
if data.get("items"):
    item = data["items"][0]
    print(f"Playlist Title: {item['snippet']['title']}")
    print(f"Total Videos (according to API): {item['contentDetails']['itemCount']}")
else:
    print("Playlist not found or invalid ID.")
    exit()

# 2. Fetch all playlist items and count them
all_videos = []
next_page_token = None
while True:
    params = {
        "part": "snippet",
        "playlistId": PLAYLIST_ID,
        "maxResults": 50,
        "key": API_KEY,
        "pageToken": next_page_token
    }
    r = requests.get("https://www.googleapis.com/youtube/v3/playlistItems", params=params)
    data = r.json()
    all_videos.extend(data.get("items", []))
    next_page_token = data.get("nextPageToken")
    print(f"Fetched page, total so far: {len(all_videos)}")
    if not next_page_token:
        break

print(f"\nTotal videos actually fetched: {len(all_videos)}")