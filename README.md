# Interactive Toxicity Analysis Dashboard
This project extends Project 2's static analysis into a dynamic exploration tool with filtering and visualization. The dashboard provides 4 interactive analyses:
1. **Toxicity Distribution** - Compare toxicity score distributions across platforms using histograms and CDFs
2. **Keyword Frequency Analysis** - Examine how keyword usage differs between high and low toxicity posts
3. **Multi-Attribute Toxicity** - Break down toxicity into six dimensions (toxicity, severe_toxicity, identity_attack, insult, profanity, threat)
4. **TF-IDF Toxic Vocabulary** - Identify platform-specific toxic language using TF-IDF analysis

## How to run the dashboard
### 1. Install Python Dependencies
**Use a virtual environment**

```bash
python3 -m venv .venv
source .venv/bin/activate  
pip install -r requirements_dashboard.txt
```
### 2. Install psycopg2 (PostgreSQL Driver)
```bash
pip install psycopg2-binary --break-system-packages
```
### 3. Configure Database Connection
Create a `.env` file in the project directory and add the database connection string:
```bash
touch .env
Something like : DATABASE_URL=postgresql://username:password@host:port/database_name
```
### 4. Remote Access (SSH Tunnel)

**On the VM:**
```bash
python3 app.py
# Leave this running
```
**On the local machine (new terminal):**
```bash
ssh -L 5000:localhost:5000 username@remote-server-address
# Something like: ssh -L 5000:localhost:5000 tanchan@remote-ip
# Leave this terminal open
```
**Access the dashboard on the local machine:**
```
http://localhost:5000
```
The dashboard is now ready to be used. all the 4 analyses can be explored by setting up parameters as wished.
