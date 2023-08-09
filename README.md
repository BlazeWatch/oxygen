
# 🌬️ Oxygen: The Breath of BlazeWatch 🌬️

Oxygen is the backend code powering BlazeWatch, handling both Ingress (incoming messages) and Egress (broadcasting to social media stations) through Memphis. With data storage in Supabase and seamless integration with Postgres, it fuels the flames of fire detection.

## Configuration

First, ensure to rename `.env.example` to `.env` and replace the relevant credentials:

```bash
MEMPHIS_HOSTNAME=string
MEMPHIS_USERNAME=string
MEMPHIS_PASSWORD=string
MEMPHIS_ACCOUNT_ID=string
DB_URL=string
DB_USER=string
DB_PASSWORD=string
```

> **Note**: For the DB, make sure to use Postgres! You can acquire a free PostgreSQL database on Supabase. The `temp_readings` tables should be created on script startup. Make sure that you have a zakar-tweets station!

## Installation & Running

Build and run the Oxygen Service by executing the following commands in the root repository directory:

```bash
docker build . --no-cache -t oxygen_backend:latest
docker run oxygen_backend:latest
```

## Features

- **Ingress & Egress Handling**: Seamless integration with Memphis for both incoming messages and broadcasting to various social media stations.
- **Supabase Storage**: Reliable and efficient data storage to Supabase, the heart and soul of your database needs.
- **PostgreSQL Support**: Designed to work with Postgres. Get your free PostgreSQL database on Supabase and feel the breeze!

## Contributing

Fuel the future of BlazeWatch by contributing to Oxygen. Fork, branch, and breathe life into your pull request!

## License

Oxygen is available under the MIT License. See `LICENSE` for more information.

---

Oxygen is the breath behind BlazeWatch, making fire detection as seamless as a gentle breeze. Be part of the revolution and feel the air of innovation! 🌬️
