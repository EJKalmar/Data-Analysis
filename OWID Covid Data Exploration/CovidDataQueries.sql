--Looking at Infections/Deaths

-- Template
Select Location, Date, total_cases, new_cases, total_deaths, population
From CovidData..CovidDeaths
order by 1,2


-- BY COUNTRY --


-- Looking at Total Cases vs Total Deaths
-- Shows the probability of death if you contract covid in your country
Select Location, Date, total_cases, total_deaths, (total_deaths/total_cases)*100 as DeathRate
From CovidData..CovidDeaths
--where location = 'Sweden'
order by 1,2

-- Looking at Total Cases vs Population
-- Shows what percentage of population got Covid
Select Location, Date, total_cases, population, (total_cases/population)*100 as CaseRate
From CovidData..CovidDeaths
order by 1, 2

-- Looking at Countries with Highest Infection Rate compared to Population
-- Shows what percentage of population got Covid
Select Location, Population, MAX(total_cases) as ToalInfectionCount, MAX((total_cases/population))*100 as PercentPopulationInfected
From CovidData..CovidDeaths
group by Location, Population
order by 4 desc

-- Looking at Countries with Highest Death Count per Population
-- Shows what percentage of population died from covid
Select Location, Population, MAX(cast (total_deaths as int)) as TotalDeathCount, MAX((total_deaths/population))*100 as PercentPopulationDead
From CovidData..CovidDeaths
Where continent is not null
group by Location, Population
order by 3 desc\


-- BY CONTINENT --


-- Looking at Continents with Highest Death Count
Select location, MAX(cast (total_deaths as int)) as TotalDeathCount, MAX((total_deaths/population))*100 as PercentPopulationDead
From CovidData..CovidDeaths
Where continent is null
group by location
order by 2 desc

-- Inconsistency in Data? Continents are listed beside Location but also reported seperately.
Select continent, MAX(cast (total_deaths as int)) as TotalDeathCount, MAX((total_deaths/population))*100 as PercentPopulationDead
From CovidData..CovidDeaths
Where continent is not null
group by continent
order by 2 desc


-- GLOBALLY --

select /*date,*/ sum(new_cases) as total_cases, sum(cast(new_deaths as int)) as total_deaths, sum(cast(new_deaths as int))/sum(new_cases)*100 as DeathPercentage
From CovidData..CovidDeaths
where continent is not null
--group by date
order by 1,2



--Now Looking at Vaccinations as well

--template
Select *
From CovidData..CovidDeaths dea
join CovidData..CovidVaccinations vac
	on dea.location = vac.location
	and dea.date = vac.date


--Looking at Total Population vs Vaccinations
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM(cast (vac.new_vaccinations as bigint)) OVER (Partition by dea.Location Order by dea.location, dea.date)
From CovidData..CovidDeaths dea
join CovidData..CovidVaccinations vac
	on dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null
order by 2,3

-- Using a Common Table Expression
With PopvsVac (Continent, Location, Date, Population, New_Vaccinations, RollingPeopleVaccinated)
as
	(
	Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM(cast (vac.new_vaccinations as bigint)) OVER (Partition by dea.Location Order by dea.location, dea.date) as RollingPeopleVaccinated
	From CovidData..CovidDeaths dea
	join CovidData..CovidVaccinations vac
		on dea.location = vac.location
		and dea.date = vac.date
	where dea.continent is not null
	)
Select *, RollingPeopleVaccinated/Population*100 as PercentagePopVaccinated
From PopvsVac

-- Using a Temporary Table
Drop Table if exists #PercentPopulationVaccinated
Create Table #PercentPopulationVaccinated
(
Continent nvarchar(255),
Location nvarchar(255),
Date datetime,
Population float,
New_vaccinations float,
RollingPeopleVaccinated float
)

Insert into #PercentPopulationVaccinated
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM(cast (vac.new_vaccinations as bigint)) OVER (Partition by dea.Location Order by dea.location, dea.date) as RollingPeopleVaccinated
From CovidData..CovidDeaths dea
join CovidData..CovidVaccinations vac
	on dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null

Select *, RollingPeopleVaccinated/Population*100 as PercentPopVaccinated
From #PercentPopulationVaccinated

-- Creating View to store data for visualizations
Create View PercentPopVaccinated as
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations, SUM(cast (vac.new_vaccinations as bigint)) OVER (Partition by dea.Location Order by dea.location, dea.date) as RollingPeopleVaccinated
From CovidData..CovidDeaths dea
join CovidData..CovidVaccinations vac
	on dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null

