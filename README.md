# Trave Destination Project
The aim of this project is to create a tool to assist its users to determine their next travel destination. At its current stage the project offers the possibility for the users to determine their travel destination based on weather, safety, affordability, pollution, and healt care quality worldwide utilising Tableau dashboards.

## Project steps
1. Data collection: Weather data from Meteostat library, quality of life indeces from Numbeo
2. Data pipeline: Python scripts to clean and structure the data
3. Postgres data warehouse/database: Python scripts to connect to a database and create SQL tables
4. Visualisation: Dashboards in Tableau visualising temperature and priority maps with relevant filters and supporting graphs

## Future iterations
- Adding more weather data to data warehouse: Adding ability filter weather by city.
- Web app: Functionality to implement ad-hoc requests by cities wordlwide, eliminating need to store large amounts of data.
- AI support in web app: Utilising AI to give users more details on travel destinations and things to do.

## Data warehouse
![dwh_design](https://github.com/vltnnx/Travel-Destination-Project/blob/main/warehouse_design/travel_destination_db.png?raw=true)
<br><br>

## Visualisations
Visualisation for the project created in Tableau. Find the interactive dashboards on Tableau Public via links below.

### Quality of life dashboard
Highlighting countries and adjusting colour scale on the map based on user filters. Dashboard allows one to filter destination countries based on average monthly temperature, safety, affordability, pollution, and health care quality data. Try the dashboard [through this link](https://public.tableau.com/views/TravelDestinationPriority/TravelDestinationPriority?:language=en-GB&publish=yes&:sid=&:display_count=n&:origin=viz_share_link).
![Quality of Life Dashboard](https://github.com/vltnnx/Travel-Destination-Project/blob/main/img/fig2_priority%20map.png?raw=true)

### Temperature dasboard
Dashboard visualising monthly temperature and precipitation data all around the world. Try yourself [through this link](https://public.tableau.com/views/WorldWeather_17150782341070/Dashboard?:language=en-GB&publish=yes&:sid=&:display_count=n&:origin=viz_share_link).
![Temperature Dashboard](https://github.com/vltnnx/Travel-Destination-Project/blob/main/img/fig1_weather%20map.png?raw=true)