import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta
import os

# Initialize T1 Faker
fake = Faker()

# Parameters
NUM_USERS = 50000        # unique users
NUM_EVENTS = 1000000     # total events
CHUNK_SIZE = 100000       # rows per batch
OUTPUT_FILE = "hackers_events_2024.csv"

COURSES = [
    "TOEFL Prep", "TOEIC Prep", "IELTS Prep",
    "SAT Prep", "GRE Prep", "GMAT Prep",
    "JLPT N1", "HSK Level 5", "TOPIK II",
    "Business English"
]
PLAN_TYPES = ["free_trial", "premium"]
EVENTS = [
    "user_signup",
    "course_search",
    "course_enroll",
    "lesson_start",
    "course_complete",
    "subscription_cancel",
    "user_referral"
]

# Generate user base
users = [fake.uuid4() for _ in range(NUM_USERS)]
start_date = datetime.now() - timedelta(days=365)  # 1 year ago

def generate_event_chunk(start_id, chunk_size):
    event_rows = []
    for i in range(chunk_size):
        event_id = start_id + i
        user_id = random.choice(users)
        event_name = random.choices(
            EVENTS,
            weights=[0.2, 0.15, 0.25, 0.25, 0.08, 0.05, 0.02],
            k=1
        )[0]
        timestamp = start_date + timedelta(
            days=random.randint(0, 365),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        plan_type = random.choices(PLAN_TYPES, weights=[0.7, 0.3])[0]
        course_id = random.choice(COURSES) if event_name in ["course_enroll", "lesson_start", "course_complete"] else None

        additional_props = {}
        if event_name == "course_complete":
            additional_props["completion_rate"] = 1.0
            additional_props["days_to_complete"] = random.randint(1, 60)
        elif event_name == "lesson_start":
            additional_props["lesson_number"] = random.randint(1, 30)
        elif event_name == "user_referral":
            additional_props["referral_code"] = fake.bothify(text="REF-####")

        event_rows.append({
            "event_id": event_id,
            "user_id": user_id,
            "event_name": event_name,
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "plan_type": plan_type,
            "course_id": course_id,
            "additional_properties": additional_props
        })
    return event_rows

# Remove file if exists
if os.path.exists(OUTPUT_FILE):
    os.remove(OUTPUT_FILE)

# Generate and write chunks
event_id = 1
while event_id <= NUM_EVENTS:
    chunk = generate_event_chunk(event_id, min(CHUNK_SIZE, NUM_EVENTS - event_id + 1))
    df = pd.DataFrame(chunk)
    write_mode = 'a' if os.path.exists(OUTPUT_FILE) else 'w'
    header = not os.path.exists(OUTPUT_FILE)
    df.to_csv(OUTPUT_FILE, mode=write_mode, header=header, index=False)
    print(f"Generated {min(event_id + CHUNK_SIZE - 1, NUM_EVENTS)} / {NUM_EVENTS} rows...")
    event_id += CHUNK_SIZE

print(f"Finished! File saved as {OUTPUT_FILE}.")
