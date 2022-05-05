/*
Covid Data Exploration

Data up to and including April 21 2022

Skills Used: Joins, Common Table Expressions, Aggregate Functions, Casting Data Types, Window Functions

*/

select * From CovidData..CovidDeaths
Where continent is not null
order by 3,4


-- Select data we want to start with

Select Location, Date, total_cases, new_cases, total_deaths, population
From CovidData..CovidDeaths
order by 1,2


-- Death Rate | Total Deaths vs. Total Cases
-- Shows the probability of death if you contracted covid in your country on a given day

Select Location, Date, total_cases, total_deaths, (total_deaths/total_cases)*100 as DeathRate
From CovidData..CovidDeaths
where location = 'Canada'
and continent is not null
order by 1,2


-- Infection Rate | Total Cases vs Population
-- Shows what percent of population had contracted Covid on a given day

Select Location, Date, total_cases, population, (total_cases/population)*100 as CaseRate
From CovidData..CovidDeaths
where continent is not null
order by 1, 2


-- Infection Rate | Total Cases vs Population
-- Shows what percent of population has contracted Covid as of April 21

Select Location, Population, MAX(total_cases) as ToalInfectionCount, MAX((total_cases/population))*100 as PercentPopulationInfected
From CovidData..CovidDeaths
where continent is not null
group by Location, Population
order by 4 desc


-- Death Rate | Population vs. Total Deaths
-- Shows total death count, what percent of population has died from covid

Select Location, Population, MAX(cast (total_deaths as int)) as TotalDeathCount, MAX((total_deaths/population))*100 as PercentPopulationDead
From CovidData..CovidDeaths
Where continent is not null
group by Location, Population
order by 3 desc


-- Regional Death Rate | Population vs. Total Deaths
-- Shows total continent death count, what percent of population has died from covid

Select location, MAX(cast (total_deaths as int)) as TotalDeathCount, MAX((total_deaths/population))*100 as PercentPopulationDead
From CovidData..CovidDeaths
Where continent is null and location not like '%income%' and location <> 'european union' and location <> 'World'
group by location
order by 2 desc


-- Global Numbers

select /*date,*/ sum(new_cases) as total_cases, sum(cast(new_deaths as int)) as total_deaths, sum(cast(new_deaths as int))/sum(new_cases)*100 as DeathRate
From CovidData..CovidDeaths
where continent is not null
--group by date
order by 1,2


-- Total Vaccinations | Total Population vs. Total Vaccinations
-- Shows percentage of population that has received at least one Covid vaccine dose on a given date

With TotalVac as
(
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM(cast (vac.new_vaccinations as bigint)) OVER (Partition by dea.Location Order by dea.location, dea.date) as TotalVaccinations
From CovidData..CovidDeaths dea
join CovidData..CovidVaccinations vac
	on dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null
--order by 2,3
)
Select *, TotalVaccinations/Population*100 as VaccinationRate
from TotalVac


-- Using a Temporary Table to perform the previous query

Drop Table if exists #PercentPopulationVaccinated
Create Table #PercentPopulationVaccinated
(
Continent nvarchar(255),
Location nvarchar(255),
Date datetime,
Population float,
New_vaccinations float,
TotalVaccinations float
)

Insert into #PercentPopulationVaccinated
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM(cast (vac.new_vaccinations as bigint)) OVER (Partition by dea.Location Order by dea.location, dea.date) as TotalVaccinations
From CovidData..CovidDeaths dea
join CovidData..CovidVaccinations vac
	on dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null

Select *, TotalVaccinations/Population*100 as PercentPopVaccinated
From #PercentPopulationVaccinated
order by 2, 3


-- Create View to store data for later visualization

Create View PercentPopVaccinated as
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM(cast (vac.new_vaccinations as bigint)) OVER (Partition by dea.Location Order by dea.location, dea.date) as TotalVaccinations
From CovidData..CovidDeaths dea
join CovidData..CovidVaccinations vac
	on dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null


/*
Notes:

'Where continent is not null' Included with all queries looking at country metrics, since continent data has been reported separately as a location.

Some metrics, for example 'total_deaths', are of type nvarchar(255) and must be cast to int to be used in aggregate functions.

'European Union' regional data is already included in 'Europe' and has been omitted when looking at regional data.

*/
