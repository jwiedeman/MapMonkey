# Google Maps Scraper Worker

This worker scrapes business listings from Google Maps using Playwright.
Results can be written to Postgres, Cassandra, a local SQLite file or a
CSV depending on the configured storage backend. Each record notes the search term and the GPS
coordinates where it was collected.

## Usage

Install Python 3.11+ and the dependencies. It's recommended to use a virtual
environment so the Cassandra driver is installed for the correct interpreter.
The driver currently supports up to Python 3.12.

```bash
python3.12 -m venv venv  # or python3.11
source venv/bin/activate
pip install -r requirements.txt
```

Copy `.env.example` to `.env` and modify values as needed for your environment.

The scraper now defaults to Cassandra. Choose between `cassandra`, `postgres`, `sqlite` or `csv`
using the `MAPS_STORAGE` environment variable or the `--store` option. When using Postgres
set the connection string with the `POSTGRES_DSN` environment variable. The Cassandra
driver is required when the storage mode is set to `cassandra`.

When using Cassandra you can configure connection parameters with the following
environment variables:

- `CASSANDRA_CONTACT_POINTS` – comma separated list of hosts
- `CASSANDRA_PORT` – port number (default `9042`)
- `CASSANDRA_KEYSPACE` – keyspace name (default `maps`)
- `CASSANDRA_LOCAL_DATA_CENTER` – data center name (default `datacenter1`)


## City grid scraping

`grid_worker.py` automates searches across a grid of GPS coordinates around a
city. Coordinates are retrieved directly from Google Maps so no external
geocoding service is required. The worker deduplicates results using business
name and address and appends them to the configured database or CSV file.

```bash
python grid_worker.py "Portland, OR" 0 0.02 50 --query "coffee shops"
```

The city name is automatically prepended to the query so the example above
searches for `"Portland, OR coffee shops"`.

The parameters are: city name, number of grid steps from the center (use `0` for a single location), spacing in degrees between grid points, number of results per grid cell, and an optional database DSN. Provide a search term with `--query`. Use `--headless` to hide the browser and `--min-delay`/`--max-delay` to randomize pauses between grid locations.

To scrape multiple terms sequentially with a single worker use the `--terms` option:

```bash
python grid_worker.py "Portland, OR" 0 0.02 50 --terms "restaurants,bars,cafes"
```

Each term is searched alongside the city, e.g. `"Portland, OR restaurants"`.

### Running multiple terms

`orchestrator.py` focuses on one city at a time but can open several browser
windows to divide the search terms among them. All terms for the first city are
processed before moving on to the next, making it easier to resume or skip
problematic cities. Progress is stored in `run_state.json` so interrupted runs

can pick up where they left off. Errors on individual terms are logged and the
remaining terms continue so a single failure doesn't abort a city. Provide a
comma separated list of search terms and use `--concurrency` to control the
number of concurrent windows. Additional cities can

be scraped by supplying `--cities` with a comma separated list in addition to
the primary positional city argument. The city name is automatically appended to
each search query.

```bash
python orchestrator.py "Portland, OR" \
  --cities "Seattle, WA,Boise, ID" \
  --terms "Restaurants,Bars,Hotels,Retail stores,Gas stations,Pharmacies,Automotive,Banks,Healthcare,Professional services,Education,Government offices,Entertainment,Construction,Real estate" \
  --steps 0 \
  --concurrency 3
```

Windows open in non‑headless mode so you can watch progress. Each browser works
through a subset of the terms for the current city until all have completed,
after which the next city begins. Specify `--state-file` to store the run state
at a different path or delete the file to start from the beginning.


## Manual monitor mode

`monitor_worker.py` lets you pan the map yourself while the script records
any new business listings that appear in the sidebar. Each unique listing is
stored in the configured database and a brief toast notification is shown in the
browser when it's saved.

```bash
python monitor_worker.py "coffee shops near me"
```

Use `--interval` to change how often the sidebar is scanned and `--headless` to
run the browser without a visible window. The `--store` option selects the
storage backend just like the other workers.

### Local Postgres setup

The workers expect a running Postgres instance.
Ensure the PostgreSQL command line tools (`initdb`, `pg_ctl` and `createdb`)
are installed and available on your `PATH`. On macOS install them with
Homebrew (`brew install postgresql`) and on Debian/Ubuntu use
`sudo apt-get install postgresql`. If the commands aren't on your `PATH` after
installing with Homebrew, add `/usr/local/opt/postgresql/bin` (or
`/opt/homebrew/opt/postgresql/bin` on Apple&nbsp;Silicon) to the `PATH`.

Run `start_postgres.sh` in this folder to initialise and launch the database.
The script automatically checks the common Homebrew locations above when
locating the Postgres tools. It creates a data directory under `pgdata/` on the
first run and starts the
server on port `5432` (or `$PGPORT` if set).

```bash
./start_postgres.sh
```

Once the server is running the default DSN `dbname=maps user=postgres host=localhost password=postgres`
will connect successfully. You can also set a custom connection string via the
`POSTGRES_DSN` environment variable when invoking the workers.

### Exporting to Excel

`export_to_excel.py` can convert a Postgres database to an Excel file:

```bash
python export_to_excel.py "dbname=maps user=postgres host=localhost password=postgres" results.xlsx
```

### Importing existing SQLite databases

`import_sqlite_to_cassandra.py` copies any `*.db` files in this folder into the
Cassandra `maps.businesses` table. Run it once after collecting data locally:

```bash
python import_sqlite_to_cassandra.py
```

## Docker Swarm

This image can also run as a service in Docker Swarm after being built and pushed to your registry.

```bash
docker service create --name <service-name> --env-file .env <image>:latest
```

Alternatively include the service in a stack file and deploy with `docker stack deploy`.




```bash
python orchestrator.py "Portland OR" \
  --cities "Aberdeen WA,Airway Heights WA,Albion WA,Algona WA,Almira WA,Anacortes WA,Arlington WA,Asotin WA,Auburn WA,Bainbridge Island WA,Battle Ground WA,Beaux Arts Village WA,Bellevue WA,Bellingham WA,Benton City WA,Bingen WA,Black Diamond WA,Blaine WA,Bonney Lake WA,Bothell WA,Brewster WA,Bridgeport WA,Brier WA,Buckley WA,Bucoda WA,Burlington WA,Camas WA,Carbonado WA,Carnation WA,Cashmere WA,Castle Rock WA,Cathlamet WA,Centralia WA,Chehalis WA,Chelan WA,Cheney WA,Chewelah WA,Clarkston WA,Cle Elum WA,Clyde Hill WA,Colfax WA,Colton WA,College Place WA,Colville WA,Concrete WA,Connell WA,Conconully WA,Cosmopolis WA,Coulee City WA,Coulee Dam WA,Coupeville WA,Covington WA,Creston WA,Cusick WA,Davenport WA,Dayton WA,Deer Park WA,Des Moines WA,DuPont WA,Duvall WA,East Wenatchee WA,Eatonville WA,Edgewood WA,Edmonds WA,Electric City WA,Ellensburg WA,Elma WA,Elmer City WA,Endicott WA,Entiat WA,Enumclaw WA,Ephrata WA,Everett WA,Everson WA,Fairfield WA,Farmington WA,Federal Way WA,Ferndale WA,Fife WA,Fircrest WA,Forks WA,Friday Harbor WA,Garfield WA,George WA,Gig Harbor WA,Gold Bar WA,Goldendale WA,Grand Coulee WA,Grandview WA,Granger WA,Granite Falls WA,Hamilton WA,Harrington WA,Harrah WA,Hartline WA,Hatton WA,Hoquiam WA,Hunts Point WA,Ilwaco WA,Index WA,Ione WA,Issaquah WA,Kahlotus WA,Kalama WA,Kelso WA,Kenmore WA,Kennewick WA,Kent WA,Kettle Falls WA,Kirkland WA,Kittitas WA,La Center WA,La Conner WA,Lacey WA,Lake Forest Park WA,Lake Stevens WA,Lakewood WA,Langley WA,Leavenworth WA,Liberty Lake WA,Lind WA,Long Beach WA,Longview WA,Lyman WA,Lynden WA,Lynnwood WA,Mabton WA,Malden WA,Mansfield WA,Maple Valley WA,Marcus WA,Marysville WA,Mattawa WA,McCleary WA,Medical Lake WA,Medina WA,Mercer Island WA,Mesa WA,Metaline WA,Metaline Falls WA,Mill Creek WA,Milton WA,Monroe WA,Montesano WA,Morton WA,Moses Lake WA,Mossyrock WA,Mount Vernon WA,Mountlake Terrace WA,Moxee WA,Mukilteo WA,Naches WA,Napavine WA,Nespelem WA,Newcastle WA,Newport WA,Nooksack WA,Normandy Park WA,North Bend WA,North Bonneville WA,Northport WA,Oak Harbor WA,Oakville WA,Ocean Shores WA,Odessa WA,Okanogan WA,Omak WA,Oroville WA,Orting WA,Othello WA,Pacific WA,Palouse WA,Pasco WA,Pateros WA,Pe Ell WA,Port Angeles WA,Port Orchard WA,Port Townsend WA,Poulsbo WA,Prescott WA,Prosser WA,Pullman WA,Puyallup WA,Quincy WA,Rainier WA,Raymond WA,Reardan WA,Redmond WA,Renton WA,Republic WA,Richland WA,Ridgefield WA,Ritzville WA,Rock Island WA,Rosalia WA,Roslyn WA,Royal City WA,Ruston WA,St. John WA,Sammamish WA,SeaTac WA,Seattle WA,Sedro-Woolley WA,Selah WA,Sequim WA,Shelton WA,Shoreline WA,Skykomish WA,Snohomish WA,Snoqualmie WA,Soap Lake WA,South Bend WA,South Cle Elum WA,South Prairie WA,Spangle WA,Spokane WA,Spokane Valley WA,Sprague WA,Springdale WA,Stanwood WA,Starbuck WA,Steilacoom WA,Stevenson WA,Sultan WA,Sumas WA,Sumner WA,Sunnyside WA,Tacoma WA,Tekoa WA,Tenino WA,Tieton WA,Toledo WA,Tonasket WA,Toppenish WA,Tukwila WA,Tumwater WA,Twisp WA,Union Gap WA,Uniontown WA,Vader WA,Vancouver WA,Waitsburg WA,Walla Walla WA,Wapato WA,Warden WA,Washougal WA,Washtucna WA,Waterville WA,Wenatchee WA,West Richland WA,Westport WA,White Salmon WA,Wilbur WA,Wilkeson WA,Wilson Creek WA,Winlock WA,Winthrop WA,Woodinville WA,Woodway WA,Yacolt WA,Yakima WA,Yarrow Point WA,Yelm WA,Zillah WA, Adair Village OR,Adams OR,Albany OR,Amity OR,Antelope OR,Arlington OR,Ashland OR,Astoria OR,Athena OR,Aumsville OR,Aurora OR,Baker City OR,Bandon OR,Banks OR,Bay City OR,Beaverton OR,Bend OR,Boardman OR,Bonanza OR,Brookings OR,Brownsville OR,Burns OR,Butte Falls OR,Canby OR,Cannon Beach OR,Carlton OR,Cascade Locks OR,Cave Junction OR,Central Point OR,Chiloquin OR,Coburg OR,Columbia City OR,Coos Bay OR,Coquille OR,Cornelius OR,Corvallis OR,Cove OR,Creswell OR,Culver OR,Dallas OR,Dayton OR,Depoe Bay OR,Detroit OR,Donald OR,Drain OR,Dufur OR,Dundee OR,Dunes City OR,Eagle Point OR,Elgin OR,Elkton OR,Enterprise OR,Estacada OR,Eugene OR,Fairview OR,Falls City OR,Florence OR,Fossil OR,Garibaldi OR,Gates OR,Gearhart OR,Gaston OR,Gervais OR,Gladstone OR,Glendale OR,Gold Beach OR,Gold Hill OR,Grants Pass OR,Grass Valley OR,Gresham OR,Haines OR,Halfway OR,Halsey OR,Happy Valley OR,Harrisburg OR,Helix OR,Heppner OR,Hermiston OR,Hillsboro OR,Hines OR,Hood River OR,Hubbard OR,Huntington OR,Idanha OR,Imbler OR,Independence OR,Ione OR,Irrigon OR,Island City OR,Jacksonville OR,Jefferson OR,John Day OR,Jordan Valley OR,Joseph OR,Junction City OR,Keizer OR,King City OR,Klamath Falls OR,La Grande OR,La Pine OR,Lafayette OR,Lake Oswego OR,Lakeside OR,Lakeview OR,Lebanon OR,Lincoln City OR,Lonerock OR,Long Creek OR,Lostine OR,Lowell OR,Lyons OR,Madras OR,Malin OR,Manzanita OR,Maupin OR,Maywood Park OR,McMinnville OR,Medford OR,Merrill OR,Metolius OR,Mill City OR,Millersburg OR,Milton-Freewater OR,Milwaukie OR,Mitchell OR,Molalla OR,Monmouth OR,Monroe OR,Monument OR,Moro OR,Mosier OR,Mount Angel OR,Myrtle Creek OR,Myrtle Point OR,Nehalem OR,Newberg OR,Newport OR,North Bend OR,North Plains OR,North Powder OR,Oakland OR,Oakridge OR,Ontario OR,Oregon City OR,Paisley OR,Pendleton OR,Philomath OR,Phoenix OR,Pilot Rock OR,Port Orford OR,Portland OR,Powers OR,Prairie City OR,Prescott OR,Prineville OR,Rainier OR,Redmond OR,Reedsport OR,Richland OR,Riddle OR,Rockaway Beach OR,Rogue River OR,Roseburg OR,Rufus OR,St. Helens OR,St. Paul OR,Salem OR,Sandy OR,Scappoose OR,Scio OR,Scotts Mills OR,Seaside OR,Seneca OR,Sheridan OR,Sherwood OR,Siletz OR,Silverton OR,Sisters OR,Sodaville OR,Spray OR,Springfield OR,Stanfield OR,Stayton OR,Sublimity OR,Sumpter OR,Summerville OR,Sutherlin OR,Sweet Home OR,Talent OR,Tangent OR,The Dalles OR,Tigard OR,Tillamook OR,Toledo OR,Troutdale OR,Tualatin OR,Turner OR,Ukiah OR,Umatilla OR,Union OR,Vale OR,Veneta OR,Vernonia OR,Waldport OR,Wallowa OR,Warrenton OR,Wasco OR,West Linn OR,Westfir OR,Willamina OR,Wilsonville OR,Winston OR,Wood Village OR,Woodburn OR,Yachats OR,Yamhill OR,Yoncalla OR, Adelanto CA,Agoura Hills CA,Alameda CA,Albany CA,Alhambra CA,Aliso Viejo CA,Alturas CA,Amador City CA,American Canyon CA,Anaheim CA,Anderson CA,Angels Camp CA,Antioch CA,Apple Valley CA,Arcadia CA,Arcata CA,Arroyo Grande CA,Artesia CA,Arvin CA,Atascadero CA,Atherton CA,Atwater CA,Auburn CA,Avalon CA,Avenal CA,Azusa CA,Bakersfield CA,Baldwin Park CA,Banning CA,Barstow CA,Beaumont CA,Bell CA,Bell Gardens CA,Bellflower CA,Belmont CA,Belvedere CA,Benicia CA,Berkeley CA,Beverly Hills CA,Big Bear Lake CA,Biggs CA,Bishop CA,Blue Lake CA,Blythe CA,Bradbury CA,Brawley CA,Brea CA,Brentwood CA,Brisbane CA,Buellton CA,Buena Park CA,Burbank CA,Burlingame CA,Calabasas CA,Calexico CA,California City CA,Calimesa CA,Calipatria CA,Calistoga CA,Camarillo CA,Campbell CA,Canyon Lake CA,Capitola CA,Carlsbad CA,Carmel-by-the-Sea CA,Carson CA,Cathedral City CA,Ceres CA,Cerritos CA,Chico CA,Chino CA,Chino Hills CA,Chowchilla CA,Chula Vista CA,Citrus Heights CA,Claremont CA,Clayton CA,Clearlake CA,Cloverdale CA,Clovis CA,Coachella CA,Coalinga CA,Colfax CA,Colma CA,Colton CA,Commerce CA,Compton CA,Concord CA,Corcoran CA,Corning CA,Corona CA,Coronado CA,Corte Madera CA,Costa Mesa CA,Cotati CA,Covina CA,Crescent City CA,Cudahy CA,Culver City CA,Cupertino CA,Cypress CA,Daly City CA,Dana Point CA,Danville CA,Davis CA,Del Mar CA,Del Rey Oaks CA,Delano CA,Desert Hot Springs CA,Diamond Bar CA,Dinuba CA,Dixon CA,Dorris CA,Dos Palos CA,Downey CA,Duarte CA,Dublin CA,Dunsmuir CA,East Palo Alto CA,Eastvale CA,El Cajon CA,El Centro CA,El Cerrito CA,El Monte CA,El Segundo CA,Elk Grove CA,Emeryville CA,Encinitas CA,Escalon CA,Escondido CA,Etna CA,Eureka CA,Exeter CA,Fairfax CA,Fairfield CA,Farmersville CA,Ferndale CA,Fillmore CA,Firebaugh CA,Folsom CA,Fontana CA,Fort Bragg CA,Fort Jones CA,Fortuna CA,Foster City CA,Fountain Valley CA,Fowler CA,Fremont CA,Fresno CA,Fullerton CA,Galt CA,Garden Grove CA,Gardena CA,Gilroy CA,Glendale CA,Glendora CA,Goleta CA,Gonzales CA,Grand Terrace CA,Grass Valley CA,Greenfield CA,Gridley CA,Grover Beach CA,Guadalupe CA,Gustine CA,Half Moon Bay CA,Hanford CA,Hawaiian Gardens CA,Hawthorne CA,Hayward CA,Healdsburg CA,Hercules CA,Hermosa Beach CA,Hesperia CA,Hidden Hills CA,Highland CA,Hillsborough CA,Hollister CA,Holtville CA,Hughson CA,Huntington Beach CA,Huntington Park CA,Huron CA,Imperial CA,Imperial Beach CA,Indian Wells CA,Indio CA,Industry CA,Inglewood CA,Ione CA,Irvine CA,Irwindale CA,Isleton CA,Jackson CA,Jurupa Valley CA,Kerman CA,King City CA,Kingsburg CA,La Cañada Flintridge CA,La Habra CA,La Habra Heights CA,La Mesa CA,La Mirada CA,La Palma CA,La Puente CA,La Quinta CA,La Verne CA,Lafayette CA,Laguna Beach CA,Laguna Hills CA,Laguna Niguel CA,Laguna Woods CA,Lake Elsinore CA,Lake Forest CA,Lakeport CA,Lakewood CA,Lancaster CA,Larkspur CA,Lathrop CA,Lawndale CA,Lemon Grove CA,Lemoore CA,Lincoln CA,Lindsay CA,Live Oak CA,Livermore CA,Livingston CA,Lodi CA,Loma Linda CA,Lomita CA,Lompoc CA,Long Beach CA,Loomis CA,Los Alamitos CA,Los Altos CA,Los Altos Hills CA,Los Angeles CA,Los Banos CA,Los Gatos CA,Loyalton CA,Lynwood CA,Madera CA,Malibu CA,Mammoth Lakes CA,Manhattan Beach CA,Manteca CA,Maricopa CA,Marina CA,Martinez CA,Marysville CA,Maywood CA,McFarland CA,Mendota CA,Menifee CA,Menlo Park CA,Merced CA,Mill Valley CA,Millbrae CA,Milpitas CA,Mission Viejo CA,Modesto CA,Monrovia CA,Montague CA,Montclair CA,Monte Sereno CA,Montebello CA,Monterey CA,Monterey Park CA,Moorpark CA,Moraga CA,Moreno Valley CA,Morgan Hill CA,Morro Bay CA,Mount Shasta CA,Mountain View CA,Murrieta CA,Napa CA,National City CA,Needles CA,Nevada City CA,Newark CA,Newport Beach CA,Norco CA,Norwalk CA,Novato CA,Oakdale CA,Oakland CA,Oakley CA,Oceanside CA,Ojai CA,Ontario CA,Orange CA,Orange Cove CA,Orinda CA,Oroville CA,Oxnard CA,Pacific Grove CA,Pacifica CA,Palm Desert CA,Palm Springs CA,Palmdale CA,Palo Alto CA,Palos Verdes Estates CA,Paradise CA,Paramount CA,Parlier CA,Pasadena CA,Paso Robles CA,Patterson CA,Perris CA,Petaluma CA,Pico Rivera CA,Piedmont CA,Pinole CA,Pismo Beach CA,Pittsburg CA,Placentia CA,Placerville CA,Pleasant Hill CA,Pleasanton CA,Plymouth CA,Point Arena CA,Pomona CA,Port Hueneme CA,Porterville CA,Portola CA,Portola Valley CA,Poway CA,Rancho Cordova CA,Rancho Cucamonga CA,Rancho Mirage CA,Rancho Palos Verdes CA,Rancho Santa Margarita CA,Red Bluff CA,Redding CA,Redlands CA,Redondo Beach CA,Redwood City CA,Reedley CA,Rialto CA,Richmond CA,Ridgecrest CA,Rio Dell CA,Rio Vista CA,Ripon CA,Riverbank CA,Riverside CA,Rocklin CA,Rohnert Park CA,Rolling Hills CA,Rolling Hills Estates CA,Rosemead CA,Roseville CA,Ross CA,Sacramento CA,Salinas CA,San Anselmo CA,San Bernardino CA,San Bruno CA,San Carlos CA,San Clemente CA,San Diego CA,San Dimas CA,San Fernando CA,San Francisco CA,San Gabriel CA,San Jacinto CA,San Joaquin CA,San Jose CA,San Juan Bautista CA,San Juan Capistrano CA,San Leandro CA,San Luis Obispo CA,San Marcos CA,San Marino CA,San Mateo CA,San Pablo CA,San Rafael CA,San Ramon CA,Sand City CA,Sanger CA,Santa Ana CA,Santa Barbara CA,Santa Clara CA,Santa Clarita CA,Santa Cruz CA,Santa Fe Springs CA,Santa Maria CA,Santa Monica CA,Santa Paula CA,Santa Rosa CA,Santee CA,Saratoga CA,Sausalito CA,Scotts Valley CA,Seal Beach CA,Seaside CA,Sebastopol CA,Selma CA,Shafter CA,Shasta Lake CA,Sierra Madre CA,Signal Hill CA,Simi Valley CA,Solana Beach CA,Soledad CA,Solvang CA,Sonoma CA,Sonora CA,South El Monte CA,South Gate CA,South Lake Tahoe CA,South Pasadena CA,South San Francisco CA,St. Helena CA,Stanton CA,Stockton CA,Suisun City CA,Sunnyvale CA,Susanville CA,Sutter Creek CA,Taft CA,Tehachapi CA,Tehama CA,Temecula CA,Temple City CA,Thousand Oaks CA,Tiburon CA,Torrance CA,Tracy CA,Trinidad CA,Truckee CA,Turlock CA,Tulelake CA,Tustin CA,Twentynine Palms CA,Ukiah CA,Union City CA,Upland CA,Vacaville CA,Vallejo CA,Ventura CA,Vernon CA,Victorville CA,Villa Park CA,Visalia CA,Vista CA,Walnut CA,Walnut Creek CA,Wasco CA,Waterford CA,Watsonville CA,Weed CA,West Covina CA,West Hollywood CA,West Sacramento CA,Westlake Village CA,Westminster CA,Westmorland CA,Wheatland CA,Whittier CA,Wildomar CA,Williams CA,Willits CA,Willows CA,Windsor CA,Winters CA,Woodlake CA,Woodland CA,Woodside CA,Yorba Linda CA,Yountville CA,Yreka CA,Yuba City CA,Yucaipa CA,Yucca Valley CA"
```
