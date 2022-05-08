/*
Covid Data Queries used for Tableau Visualizations

You can check out the visualizations on my Tableau Public Profile: https://public.tableau.com/app/profile/jonathan.kalmar

*/


-- #1 COVID SNAPSHOT 2021-04-21


-- 1) Global Numbers

select SUM(new_cases) as total_cases, SUM(cast(new_deaths as int)) as total_deaths, SUM(cast(new_deaths as int))/sum(new_cases)*100 as death_rate
From CovidData..CovidDeaths
Where continent is not null


-- 2) Regional Deaths

Select location, SUM(cast (new_deaths as int)) as total_deaths, SUM(cast (new_deaths as int))/SUM(new_cases)*100 as death_rate
From CovidData..CovidDeaths
Where continent is null and location not like '%income%' and location not in ('World', 'European Union', 'International')
Group by location
Order by 2 desc


--3) Current Infection Rate Per Country

Select location, population, MAX(total_cases) as total_cases, MAX((total_cases/population))*100 as infection_rate
From CovidData..CovidDeaths
Where continent is not null
Group by location, population
Order by 4 desc


--4) Daily Infection Rates Per Country

Select location, population, date, MAX(total_cases) as total_cases, MAX((total_cases/population))*100 as infection_rate
From CovidData..CovidDeaths
Where continent is not null
Group by location, population, date
Order by 5 desc



-- #2 COVID FACTORS AFFECTING DEATH RATE


--1) Date, Vaccination Rates vs. Death Rates

Select dea.location, dea.date, dea.total_deaths, dea.total_cases, dea.total_deaths/dea.total_cases*100 as death_rate, vac.people_vaccinated, vac.people_vaccinated/population*100 as vaccination_rate
From CovidData..CovidDeaths dea
Join CovidData..CovidVaccinations vac
On dea.location = vac.location and dea.date = vac.date
Where dea.continent is not null or dea.location = 'World'
Order by 1,3


--2) Income vs. Death Rate

Select dea.location, dea.population, MAX(cast (dea.total_deaths as int)) as total_deaths, MAX(dea.total_cases) as total_cases, MAX(cast (dea.total_deaths as int))/MAX(dea.total_cases)*100 as death_rate
From CovidData..CovidDeaths dea
Join CovidData..CovidVaccinations vac
On dea.location = vac.location and dea.date = vac.date
Where dea.location like '%income%'
Group by dea.location, dea.population
Order by 5 desc


--3) Gdp per capita, Median Age, Diabetes , Smokers vs. Death Rate

Select dea.location, dea.population, vac.gdp_per_capita, vac.median_age, vac.diabetes_prevalence, male_smokers, female_smokers, MAX(cast (dea.total_deaths as int)) as total_deaths, MAX(dea.total_cases) as total_cases, MAX(cast (dea.total_deaths as int))/MAX(dea.total_cases)*100 as death_rate
From CovidData..CovidDeaths dea
Join CovidData..CovidVaccinations vac
On dea.location = vac.location and dea.date = vac.date
Where dea.continent is not null
Group by dea.location, dea.population, vac.gdp_per_capita, vac.median_age, vac.diabetes_prevalence, male_smokers, female_smokers
Order by 10 desc

/*
NOTE:

Countries with missing/NULL values for a given metric (ex. GDP Per Capita) were excluded from analysis.

*/
