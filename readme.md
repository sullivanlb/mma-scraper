# MMA Scraper

### Actual Process
1. Update recent event (7 days before + 7 days after)
2. For each event found => update_single_event()
    1. Create the event record if it doesn't exist in db
    2. If it exist, update it if needed (by calculating hash)
        1. Update event basic data then fights then check/add new fights
3. Update all fighter in db that need update (all fighter for the moment)
4. For each fighter found => update_single_fighter()
    5. Get fighter in db, check hash
        1. If hash is different it update else it go next fighter
        2. If update, update basic data then update fighter fights of events in db, if opponent doesn't exist it get created

### Next Process
#### Phase 1: Initial Full Scraping (One-time)
1. Get all UFC events (historical + future)
2. For each event chronologically:
   - Create/update event record
   - Process all fights in event
   - Create/update fighters as needed
3. Mark database as "fully initialized"

#### Phase 2: Incremental Updates (Ongoing)
1. Check for new/updated events (last 30 days for safety)
2. For each changed event:
   - Update event if hash differs
   - Process fight changes
3. Smart fighter updates:
   - Only update fighters who had recent fights
   - Or fighters whose basic data hash changed
   - Or fighters flagged for update

### Event
- name
- datetime
- promotion
- venue
- location
- mma_bouts
- img_url
- broadcast

### Fighter data
- nickname
- date_of_birth
- weight_class 
- last_weight_in
- born
- head_coach
- pro_mma_record
- current_mma_streak
- affiliation
- other_coaches
- last_fight_date
- total_fights
- age

### Fights
- id_event
- id_fighter_1
- id_fighter_2
- result_fighter_1
- result_fighter_2
- finish_by
- finish_by_details
- minutes_per_rounds
- rounds
- fight_type

### Records by promotion (Coming soon)
- from
- to
- promotion
- win
- loss
- draw
- no_contest
- win_ko
- win_sub
- win_decision
- win_dq
- loss_ko
- loss_sub
- loss_decision
- loss_dq
- id_fighter

### Fighters social links (Coming soon)