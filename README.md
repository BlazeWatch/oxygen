# oxygen
Backend Code for Blaze. Deals with both Ingress(incoming messages) and Egress(broadcasting to social media stations) with Memphis and saves data to Supabase. 


First, make sure to rename .env.example to .env and replace with the relavant credentials!

Note: For the DB, make sure to use Postgres! The temp_readings tables should be created on script startup. Make sure that you have a zakar-tweets station!
```bash
MEMPHIS_HOSTNAME=string
MEMPHIS_USERNAME=string
MEMPHIS_PASSWORD=string
MEMPHIS_ACCOUNT_ID=string
DB_URL=string
DB_USER=string
DB_PASSWORD=string
```

You can run the Oxygen Service by building and running the Docker container in the root repository directory:

```bash
docker build . --no-cache -t oxygen_backend:latest 
docker run oxygen_backend:latest 
```