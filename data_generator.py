#!/usr/bin/env python3

import sys
import time
import uuid
import random
import json
import argparse
from datetime import datetime, timezone
from typing import List, Dict, Any
import psycopg2

# Sample data for content generation
CONTENT_TYPES = ["podcast", "newsletter", "video"]
PODCAST_TITLES = [
    "Tech Talk Weekly",
    "Startup Stories",
    "Code & Coffee",
    "Innovation Hub",
    "Digital Trends",
    "Future Forward",
    "Developer Diaries",
    "AI Insights",
]
NEWSLETTER_TITLES = [
    "Morning Brief",
    "Weekly Roundup",
    "Industry Update",
    "Market Watch",
    "Tech Digest",
    "Startup News",
    "Product Updates",
    "Leadership Notes",
]
VIDEO_TITLES = [
    "Tutorial: Getting Started",
    "Product Demo",
    "Behind the Scenes",
    "Expert Interview",
    "Case Study",
    "Live Q&A",
    "Workshop Recording",
]

EVENT_TYPES = ["play", "pause", "finish", "click"]
DEVICES = ["ios", "android", "web-chrome", "web-safari", "web-firefox", "desktop"]


class DataGenerator:
    def __init__(self, db_config):
        self.db_config = db_config
        self.content_cache = []
        self.user_ids = []

    def connect_db(self):
        return psycopg2.connect(**self.db_config)

    def load_existing_content(self):
        """Load existing content IDs for engagement events"""
        with self.connect_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM content")
                self.content_cache = [row[0] for row in cur.fetchall()]
        print(f"Loaded {len(self.content_cache)} existing content items")

    def generate_user_ids(self, count=1000):
        """Generate pool of user IDs for engagement events"""
        self.user_ids = [str(uuid.uuid4()) for _ in range(count)]

    def generate_content_row(self) -> Dict[str, Any]:
        """Generate a single content row"""
        content_type = random.choice(CONTENT_TYPES)

        if content_type == "podcast":
            title = random.choice(PODCAST_TITLES)
            length_seconds = random.randint(1200, 7200)  # 20min to 2hrs
        elif content_type == "newsletter":
            title = random.choice(NEWSLETTER_TITLES)
            length_seconds = None  # newsletters don't have duration
        else:  # video
            title = random.choice(VIDEO_TITLES)
            length_seconds = random.randint(300, 3600)  # 5min to 1hr

        # Create slug from title
        slug = (
            title.lower().replace(" ", "-").replace(":", "")
            + f"-{random.randint(1000, 9999)}"
        )

        return {
            "id": str(uuid.uuid4()),
            "slug": slug,
            "title": title,
            "content_type": content_type,
            "length_seconds": length_seconds,
            "publish_ts": datetime.now(timezone.utc),
        }

    def generate_engagement_row(self, content_pool=None) -> Dict[str, Any]:
        """Generate a single engagement event row"""
        # Use provided content pool or fall back to cache
        available_content = content_pool if content_pool else self.content_cache

        if not available_content:
            print("Warning: No content available for engagement events")
            return None

        content_id = random.choice(available_content)
        user_id = random.choice(self.user_ids)
        event_type = random.choice(EVENT_TYPES)
        device = random.choice(DEVICES)

        # Generate realistic duration based on event type
        duration_ms = None
        if event_type in ["play", "pause"]:
            duration_ms = random.randint(1000, 300000)  # 1sec to 5min

        # Generate some realistic raw payload
        raw_payload = {
            "session_id": str(uuid.uuid4()),
            "user_agent": f"{device}-client",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if event_type == "click":
            raw_payload["click_position"] = random.randint(1, 100)

        return {
            "content_id": content_id,
            "user_id": user_id,
            "event_type": event_type,
            "event_ts": datetime.now(timezone.utc),
            "duration_ms": duration_ms,
            "device": device,
            "raw_payload": json.dumps(raw_payload),
        }

    def insert_content_batch(self, rows: List[Dict[str, Any]]):
        """Insert batch of content rows"""
        with self.connect_db() as conn:
            with conn.cursor() as cur:
                for row in rows:
                    cur.execute(
                        """
                        INSERT INTO content (id, slug, title, content_type, length_seconds, publish_ts)
                        VALUES (%(id)s, %(slug)s, %(title)s, %(content_type)s, %(length_seconds)s, %(publish_ts)s)
                    """,
                        row,
                    )
                    # Add to cache for engagement events
                    self.content_cache.append(row["id"])
            conn.commit()

    def insert_engagement_batch(self, rows: List[Dict[str, Any]]):
        """Insert batch of engagement event rows"""
        with self.connect_db() as conn:
            with conn.cursor() as cur:
                for row in rows:
                    if row:  # Skip None rows
                        cur.execute(
                            """
                            INSERT INTO engagement_events (content_id, user_id, event_type, event_ts, duration_ms, device, raw_payload)
                            VALUES (%(content_id)s, %(user_id)s, %(event_type)s, %(event_ts)s, %(duration_ms)s, %(device)s, %(raw_payload)s)
                        """,
                            row,
                        )
            conn.commit()

    def run_generator(
        self,
        content_per_min: int,
        engagement_per_min: int,
        duration_mins: int,
        use_existing: bool = False,
        prefer_new: bool = False,
    ):
        """Run the data generator"""
        print("Starting data generation:")
        print(f"  Content: {content_per_min} rows/minute")
        print(f"  Engagement: {engagement_per_min} rows/minute")
        print(f"  Duration: {duration_mins} minutes")
        print(
            f"  Total: {content_per_min * duration_mins} content, {engagement_per_min * duration_mins} engagement events"
        )

        # Load existing data
        self.load_existing_content()
        self.generate_user_ids()

        # Track newly created content IDs in this session
        newly_created_content = []

        start_time = time.time()

        for minute in range(duration_mins):
            minute_start = time.time()

            # Generate content rows
            if content_per_min > 0:
                content_rows = [
                    self.generate_content_row() for _ in range(content_per_min)
                ]
                self.insert_content_batch(content_rows)
                # Track new content IDs for engagement events
                newly_created_content.extend([row["id"] for row in content_rows])
                print(f"Minute {minute + 1}: Inserted {len(content_rows)} content rows")

            # Generate engagement rows - choose content pool based on flags
            if engagement_per_min > 0:
                if use_existing:
                    # Force use of existing content only
                    content_pool = self.content_cache
                    print(
                        f"         Using {len(self.content_cache)} existing content items (--use-existing)"
                    )
                elif prefer_new and newly_created_content:
                    # Prefer newly created content if available
                    content_pool = newly_created_content
                    print(
                        f"         Using {len(newly_created_content)} newly created content items (--prefer-new)"
                    )
                elif newly_created_content:
                    # Default behavior: use newly created if available
                    content_pool = newly_created_content
                    print(
                        f"         Using {len(newly_created_content)} newly created content items for engagement"
                    )
                else:
                    # Fallback to existing content
                    content_pool = self.content_cache
                    print(
                        f"         Using {len(self.content_cache)} existing content items for engagement"
                    )
                engagement_rows = [
                    self.generate_engagement_row(content_pool)
                    for _ in range(engagement_per_min)
                ]
                engagement_rows = [row for row in engagement_rows if row is not None]
                if engagement_rows:
                    self.insert_engagement_batch(engagement_rows)
                    print(
                        f"Minute {minute + 1}: Inserted {len(engagement_rows)} engagement events"
                    )

            # Wait for the rest of the minute
            elapsed = time.time() - minute_start
            if elapsed < 60:
                time.sleep(60 - elapsed)

        total_time = time.time() - start_time
        print(f"Data generation completed in {total_time:.1f} seconds")


def main():
    parser = argparse.ArgumentParser(description="Generate test data for CDC pipeline")
    parser.add_argument(
        "--content",
        "-c",
        type=int,
        default=0,
        help="Content rows per minute (default: 0)",
    )
    parser.add_argument(
        "--engagement",
        "-e",
        type=int,
        default=10,
        help="Engagement event rows per minute (default: 10)",
    )
    parser.add_argument(
        "--duration", "-d", type=int, default=5, help="Duration in minutes (default: 5)"
    )
    parser.add_argument("--host", default="localhost", help="Database host")
    parser.add_argument("--port", type=int, default=5432, help="Database port")
    parser.add_argument("--database", default="users_data", help="Database name")
    parser.add_argument("--user", default="debezium", help="Database user")
    parser.add_argument("--password", default="dbz", help="Database password")
    parser.add_argument(
        "--use-existing",
        action="store_true",
        help="Force engagement events to use existing content only (ignore newly created)",
    )
    parser.add_argument(
        "--prefer-new",
        action="store_true",
        help="Prefer newly created content for engagement events (default behavior)",
    )

    args = parser.parse_args()

    # Database configuration
    db_config = {
        "host": args.host,
        "port": args.port,
        "database": args.database,
        "user": args.user,
        "password": args.password,
    }

    # Test database connection
    try:
        generator = DataGenerator(db_config)
        with generator.connect_db() as _:
            print(f"Connected to database: {args.database}")
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        sys.exit(1)

    # Run generator
    generator.run_generator(
        args.content,
        args.engagement,
        args.duration,
        use_existing=args.use_existing,
        prefer_new=args.prefer_new,
    )


if __name__ == "__main__":
    main()
