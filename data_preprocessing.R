# Import libraries
library(tidyverse)
library(data.table)
library(RSQLite)
library(sqldf)
library(dbplyr)

# Set up SQLite database and connection
db <- dbConnect(SQLite(), dbname = "pubg_db.sqlite")

# Read in data to RSQLite database
agg_match_stats_files <- c("data/agg_match_stats_0.csv",
                           "data/agg_match_stats_1.csv",
                           "data/agg_match_stats_2.csv",
                           "data/agg_match_stats_3.csv",
                           "data/agg_match_stats_4.csv"
)

for (file in agg_match_stats_files) {
    tmp <- fread(file) %>% na.omit()
    dbWriteTable(db, "AGGMATCHSTATS", tmp, append = TRUE)
    rm(tmp)
}

# Transform data into player stats
agg_db <- tbl(db, "AGGMATCHSTATS")

player_total_stats <- agg_db %>% 
    mutate(walk_per_time = player_dist_walk / player_survive_time) %>%
    mutate(ride_per_time = player_dist_ride / player_survive_time) %>%
    group_by(player_name) %>%
    summarise(n.matches.total = n(), 
              kills.total.mean = mean(player_kills),
              kills.total.min = min(player_kills),
              kills.total.sd = sd(player_kills),
              kills.total.max = max(player_kills),
              dmg.total.mean = mean(player_dmg),
              dmg.total.min = min(player_dmg),
              dmg.total.sd = sd(player_dmg),
              dmg.total.max = max(player_dmg),
              assists.total.mean = mean(player_assists),
              assists.total.min = min(player_assists),
              assists.total.sd = sd(player_assists),
              assists.total.max = max(player_assists),
              team.placement.total.mean = mean(team_placement),
              team.placement.total.min = min(team_placement),
              team.placement.total.sd = sd(team_placement),
              team.placement.total.max = max(team_placement),
              dist.ride.per.time.total.mean = mean(ride_per_time),
              dist.ride.per.time.total.min = min(ride_per_time),
              dist.ride.per.time.total.sd = sd(ride_per_time),
              dist.ride.per.time.total.max = max(ride_per_time),
              dist.walk.per.time.total.mean = mean(walk_per_time),
              dist.walk.per.time.total.min = min(walk_per_time),
              dist.walk.per.time.total.sd = sd(walk_per_time),
              dist.walk.per.time.total.max = max(walk_per_time)
    ) %>% 
    collect()

player_solo_stats <- agg_db %>% 
    filter(party_size == 1) %>%
    mutate(walk_per_time = player_dist_walk / player_survive_time) %>%
    mutate(ride_per_time = player_dist_ride / player_survive_time) %>%
    group_by(player_name) %>%
    summarise(n.matches.solo = n(), 
              kills.solo.mean = mean(player_kills),
              kills.solo.min = min(player_kills),
              kills.solo.sd = sd(player_kills),
              kills.solo.max = max(player_kills),
              dmg.solo.mean = mean(player_dmg),
              dmg.solo.min = min(player_dmg),
              dmg.solo.sd = sd(player_dmg),
              dmg.solo.max = max(player_dmg),
              assists.solo.mean = mean(player_assists),
              assists.solo.min = min(player_assists),
              assists.solo.sd = sd(player_assists),
              assists.solo.max = max(player_assists),
              team.placement.solo.mean = mean(team_placement),
              team.placement.solo.min = min(team_placement),
              team.placement.solo.sd = sd(team_placement),
              team.placement.solo.max = max(team_placement),
              dist.ride.per.time.solo.mean = mean(ride_per_time),
              dist.ride.per.time.solo.min = min(ride_per_time),
              dist.ride.per.time.solo.sd = sd(ride_per_time),
              dist.ride.per.time.solo.max = max(ride_per_time),
              dist.walk.per.time.solo.mean = mean(walk_per_time),
              dist.walk.per.time.solo.min = min(walk_per_time),
              dist.walk.per.time.solo.sd = sd(walk_per_time),
              dist.walk.per.time.solo.max = max(walk_per_time)
    ) %>% 
    collect()

player_duo_stats <- agg_db %>% 
    filter(party_size == 2) %>%
    mutate(walk_per_time = player_dist_walk / player_survive_time) %>%
    mutate(ride_per_time = player_dist_ride / player_survive_time) %>%
    group_by(player_name) %>%
    summarise(n.matches.duo = n(), 
              kills.duo.mean = mean(player_kills),
              kills.duo.min = min(player_kills),
              kills.duo.sd = sd(player_kills),
              kills.duo.max = max(player_kills),
              dmg.duo.mean = mean(player_dmg),
              dmg.duo.min = min(player_dmg),
              dmg.duo.sd = sd(player_dmg),
              dmg.duo.max = max(player_dmg),
              assists.duo.mean = mean(player_assists),
              assists.duo.min = min(player_assists),
              assists.duo.sd = sd(player_assists),
              assists.duo.max = max(player_assists),
              team.placement.duo.mean = mean(team_placement),
              team.placement.duo.min = min(team_placement),
              team.placement.duo.sd = sd(team_placement),
              team.placement.duo.max = max(team_placement),
              dist.ride.per.time.duo.mean = mean(ride_per_time),
              dist.ride.per.time.duo.min = min(ride_per_time),
              dist.ride.per.time.duo.sd = sd(ride_per_time),
              dist.ride.per.time.duo.max = max(ride_per_time),
              dist.walk.per.time.duo.mean = mean(walk_per_time),
              dist.walk.per.time.duo.min = min(walk_per_time),
              dist.walk.per.time.duo.sd = sd(walk_per_time),
              dist.walk.per.time.duo.max = max(walk_per_time)
    ) %>% 
    collect()

player_squad_stats <- agg_db %>% 
    filter(party_size == 4) %>%
    mutate(walk_per_time = player_dist_walk / player_survive_time) %>%
    mutate(ride_per_time = player_dist_ride / player_survive_time) %>%
    group_by(player_name) %>%
    summarise(n.matches.squad = n(), 
              kills.squad.mean = mean(player_kills),
              kills.squad.min = min(player_kills),
              kills.squad.sd = sd(player_kills),
              kills.squad.max = max(player_kills),
              dmg.squad.mean = mean(player_dmg),
              dmg.squad.min = min(player_dmg),
              dmg.squad.sd = sd(player_dmg),
              dmg.squad.max = max(player_dmg),
              assists.squad.mean = mean(player_assists),
              assists.squad.min = min(player_assists),
              assists.squad.sd = sd(player_assists),
              assists.squad.max = max(player_assists),
              team.placement.squad.mean = mean(team_placement),
              team.placement.squad.min = min(team_placement),
              team.placement.squad.sd = sd(team_placement),
              team.placement.squad.max = max(team_placement),
              dist.ride.per.time.squad.mean = mean(ride_per_time),
              dist.ride.per.time.squad.min = min(ride_per_time),
              dist.ride.per.time.squad.sd = sd(ride_per_time),
              dist.ride.per.time.squad.max = max(ride_per_time),
              dist.walk.per.time.squad.mean = mean(walk_per_time),
              dist.walk.per.time.squad.min = min(walk_per_time),
              dist.walk.per.time.squad.sd = sd(walk_per_time),
              dist.walk.per.time.squad.max = max(walk_per_time)
    ) %>% 
    collect()

# Now, join all stats together (total, solo, duo and squad games) 
# -- this will introduce NAs for players that have not played in 
# a certain party_size and we will replace these NAs by 0s.

player_total_stats <-
    player_total_stats %>%
    left_join(player_solo_stats, by = "player_name") %>%
    left_join(player_duo_stats, by = "player_name") %>%
    left_join(player_squad_stats, by = "player_name")



# Create kills data and join with player_total_stats
kill_match_stats_files <- c("data/kill_match_stats_final_0.csv", 
                            "data/kill_match_stats_final_1.csv",
                            "data/kill_match_stats_final_2.csv",
                            "data/kill_match_stats_final_3.csv",
                            "data/kill_match_stats_final_4.csv"
)
for (file in kill_match_stats_files) {
    tmp <- fread(file) %>% na.omit()
    dbWriteTable(db, "KILLMATCHSTATS", tmp, append = TRUE)
    rm(tmp)
}
kills_db <- tbl(db, "KILLMATCHSTATS")

# Extract the unique weapons from all levels of the killed_by variable
weapons <- kills_db %>% 
    select(killed_by) %>% 
    collect() %>%
weapons <- as.vector(weapons$killed_by)
weapons <- weapons[!duplicated(weapons)]

# Construct and execute SQL query to one-hot-encode the categorical variable killed_by
oneHotSQL <- function(select, variable, levels, table) {
    query <- paste0("SELECT ", select,", ")
    for (i in levels) {
        part_of_query <- paste0("COUNT(CASE WHEN ", variable, " = '", i, "' THEN 1 END) AS '", i,"', ")
        query <- paste0(query, part_of_query)
    }
    query <- gsub('.{2}$', '', query)  # removes the last comma to not cause a syntax error in the SQL query
    query <- paste0(query, " FROM ", table, " GROUP BY ", select)
    return(query)
}
one_hot_query <- oneHotSQL("killer_name", "killed_by", weapons, "KILLMATCHSTATS")
killed_by_stats <- sqldf(one_hot_query, dbname = "pubg_db.sqlite")

# Group kills into weapon classes, drop self-inflicted deaths like drowning and falling and other non-kills
killed_by_stats <- 
    select(-c(Bluezone, RedZone, Drown, Falling, death.PlayerMale_A_C)) %>%
    transmute(
        assault.rifle.kills = Groza + AKM + `SCAR-L` + M16A4 + M416 + AUG,
        shotgun.kills = S12K + S1897 + S686 + death.WeapSawnoff_C,
        sniper.rifle.kills = AWM + M24 + Kar98k + VSS,
        rifle.kills = Win94,
        DMR.kills = `Mini 14` + SKS + Mk14,
        SMG.kills = `Micro UZI` + UMP9 + Vector + `Tommy Gun`,
        LMG.kills = `DP-28` + M249,
        pistol.kills = P18C + P92 + P1911 + R1895 + R45,
        grenade.kills = Grenade + death.ProjMolotov_C + death.ProjMolotov_DamageField_C,
        melee.kills = Crowbar + Pan + Machete + Punch + Sickle,
        vehicular.kills = Boat + Aquarail + Buggy + Dacia + `Hit by Car` + Motorbike + `Motorbike (SideCar)` + `Pickup Truck` + Van + Uaz + death.PG117_A_01_C,
        bow.and.arrow.kills = Crossbow
    )

# Construct kill distance stats
kill_distance_stats <-
    kills_db %>%
    mutate(kill_distance = sqrt((killer_position_x - victim_position_x)^2 + (killer_position_y - victim_position_y)^2)) %>%
    select(killer_name, kill_distance) %>%
    group_by(killer_name) %>% 
    summarise(
        kill.distance.mean = mean(kill_distance),
        kill.distance.min = min(kill_distance),
        kill.distance.sd = sd(kill_distance),
        kill.distance.max = max(kill_distance)
    ) %>%
    collect()

# Join with the killed_by stats, performing an inner join to retain only players with both killed_by and kill_distance data
kill_stats <- inner_join(killed_by_stats, kill_distance_stats, by = "killer_name")
rm(killed_by_stats, kill_distance_stats)

# ... and finally, join with the player_match_stats_total. Again, I will perform an inner join to retain a resulting dataset of high quality rather than maximum completeness, as the goal of this project is not to produce a production-ready player stats database
player_total_stats <- 
    player_total_stats %>%
    inner_join(kill_stats, by = c("player_name" = "killer_name"))

fwrite(player_total_stats, "player_total_stats_after_preprocessing.csv")