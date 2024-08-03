import pandas as pd
import json
from tqdm import tqdm
from pygments import highlight
from pygments.lexers import JsonLexer
from pygments.formatters import TerminalFormatter

from google_play_scraper import Sort, reviews, app

productivity_app_packages = [
  'com.anydo',
  'com.todoist',
  'com.ticktick.task',
  'com.habitrpg.android.habitica',
  'cc.forestapp',
  'com.oristats.habitbull',
  'com.levor.liferpgtasks',
  'com.habitnow',
  'com.microsoft.todos',
  'prox.lab.calclock',
  'com.gmail.jmartindev.timetune',
  'com.artfulagenda.app',
  'com.tasks.android',
  'com.appgenix.bizcal',
  'com.appxy.planner'
]

fitness_app_packages = [
'com.nike.plusgps',
'com.myfitnesspal.android',
'homeworkout.homeworkouts.noequipment',
'com.planetfitness',
'women.workout.female.fitness',
'sixpack.sixpackabs.absworkout',
'menloseweight.loseweightappformen.weightlossformen',
'com.stepsappgmbh.stepsapp',
'com.gymshark.fitness',
'com.the.pump',
'com.techbull.fitolympia.paid',
'com.tinymission.dailyworkoutspaid',
'com.rainfrog.yoga',
'net.kairosoft.android.boxing'
]

health_app_packages = [
'org.iggymedia.periodtracker',
'io.yuka.android',
'net.daylio',
'com.healthifyme.basic',
'com.remind.drink.water.hourly',
'com.northcube.sleepcycle',
'bodyfast.zero.fastingtracker.weightloss',
'com.extrawest.healthcare_flutter_app',
'com.eatthismuch',
'heartratemonitor.heartrate.pulse.pulseapp',
'com.google.android.apps.healthdata',
'com.oneplus.health.international'
]

app_infos = []

for ap in tqdm(productivity_app_packages):
  info = app(ap, lang='en', country='us')
  del info['comments']
  app_infos.append(info)

def print_json(json_object):
  json_str = json.dumps(
    json_object,
    indent=2,
    sort_keys=True,
    default=str
  )
  print(highlight(json_str, JsonLexer(), TerminalFormatter()))

# Scraping information for fitness apps
fitness_app_infos = []
for ap in tqdm(fitness_app_packages):
    info = app(ap, lang='en', country='us')
    del info['comments']
    fitness_app_infos.append(info)

# Scraping information for health apps
health_app_infos = []
for ap in tqdm(health_app_packages):
    info = app(ap, lang='en', country='us')
    del info['comments']
    health_app_infos.append(info)

# Creating DataFrames for each category
app_infos = pd.DataFrame(app_infos)
app_infos_df = app_infos[["title","score", "ratings", "containsAds", "price", "free","realInstalls", "released", "inAppProductPrice", "icon"]]

fitness_app_infos_df = pd.DataFrame(fitness_app_infos)
fitness_app_infos_df = fitness_app_infos_df[["title","score", "ratings", "containsAds", "price", "free","realInstalls", "released", "inAppProductPrice", "icon"]]

health_app_infos_df = pd.DataFrame(health_app_infos)
health_app_infos_df = health_app_infos_df[["title","score", "ratings", "containsAds", "price", "free","realInstalls", "released", "inAppProductPrice", "icon"]]

print(health_app_infos_df)


app_reviews = []

app_reviews = []

for ap in tqdm(productivity_app_packages):
  for score in list(range(1, 6)):
    for sort_order in [Sort.MOST_RELEVANT, Sort.NEWEST]:
      rvs, _ = reviews(
        ap,
        lang='en',
        country='us',
        sort=sort_order,
        count= 20 if score == 3 else 10,
        filter_score_with=score
      )
      for r in rvs:
        r['sortOrder'] = 'most_relevant' if sort_order == Sort.MOST_RELEVANT else 'newest'
        r['appId'] = ap
      app_reviews.extend(rvs)


# Scraping reviews for fitness apps
fitness_app_reviews = []
for ap in tqdm(fitness_app_packages):
    for score in list(range(1, 6)):
        for sort_order in [Sort.MOST_RELEVANT, Sort.NEWEST]:
            rvs, _ = reviews(
                ap,
                lang='en',
                country='us',
                sort=sort_order,
                count=200 if score == 3 else 100,
                filter_score_with=score
            )
            for r in rvs:
                r['sortOrder'] = 'most_relevant' if sort_order == Sort.MOST_RELEVANT else 'newest'
                r['appId'] = ap
            fitness_app_reviews.extend(rvs)

# Scraping reviews for health apps
health_app_reviews = []
for ap in tqdm(health_app_packages):
    for score in list(range(1, 6)):
        for sort_order in [Sort.MOST_RELEVANT, Sort.NEWEST]:
            rvs, _ = reviews(
                ap,
                lang='en',
                country='us',
                sort=sort_order,
                count=200 if score == 3 else 100,
                filter_score_with=score
            )
            for r in rvs:
                r['sortOrder'] = 'most_relevant' if sort_order == Sort.MOST_RELEVANT else 'newest'
                r['appId'] = ap
            health_app_reviews.extend(rvs)

# # Creating DataFrames for reviews of each category
app_reviews_df = pd.DataFrame(app_reviews)
fitness_app_reviews_df = pd.DataFrame(fitness_app_reviews)
health_app_reviews_df = pd.DataFrame(health_app_reviews)

reviews_df = app_reviews_df[["appId", "content", "score"]]
fitness_app_reviews_df = fitness_app_reviews_df[["appId", "content", "score"]]
health_app_reviews_df = health_app_reviews_df[["appId", "content", "score"]]
