- **`summary`** has **`base_ts`**, the max(**`base_ts`**) can be taken to identify the last updated value in **`summary`**
- Similarly the most recent entry for a **`cons_acct_key`** is kept in **`latest_summary`**


## Case I - Non Existent **`cons_acct_key`**
This is a new **`cons_acct_key`** and does not pre-exist in **`summary`** & **`latest_summary`**

### Approach - 
For such cases, we will need to create a new entry in **`summary`** & **`latest_summary`**, all the previous month entries will be NULL

## Case II - Existing **`cons_acct_key`** new month entry
This is the case where the record belongs to a **`cons_acct_key`** that already exists in **`summary`** & **`latest_summary`** but the month is new, its a entry where the **`acct_dt`** is newer than the existing **`acct_dt`** in **`latest_summary`**

### Approach - 
- For such cases, we will need to update the **`latest_summary`** with the new record and also update the **`summary`** with the new month entry
- If the entry is made post some gap(non reportings) then subsequent NULL based shifting must be done while creating the **`summary`** row.

## Case III - Existing **`cons_acct_key`** previous month entry
This is the case where the record belongs to a **`cons_acct_key`** that already exists in **`summary`** & **`latest_summary`** but the month is previous, its a entry where the **`acct_dt`** is older than the existing **`acct_dt`** in **`latest_summary`**

#### Sub Scenarios -
1. Case where the record is for a month that already exists in **`summary`**
2. Case where the record is for a month that does not exist in **`summary`** but future month entries exist (in between single month gap)
3. Case where the record is for a month that does not exist in **`summary`** but future month entries exist (in between multiple month gap)