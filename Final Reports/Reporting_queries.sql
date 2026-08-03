--- annual emissions figures for NS PA

WITH annual_emissions_savings AS(

SELECT sum(diff_break1) as emissions_savings, financial_year_end, delivery_partner
FROM pa_ghg_reporting.emission_factors_final_calcs_latest
where 
emission_factor_pre != 'Forest'
	and delivery_partner = 'NS'
group by financial_year_end, delivery_partner
order by financial_year_end
)

SELECT emissions_savings, SUM(emissions_savings) OVER (ORDER BY financial_year_end) AS cumulative_savings, financial_year_end, delivery_partner
FROM annual_emissions_savings
group by financial_year_end, delivery_partner, emissions_savings
order by financial_year_end;

--- cumulative sum total for NS PA
WITH annual_emissions_savings AS(
SELECT sum(diff_break1) as emissions_savings, financial_year_end, delivery_partner
FROM pa_ghg_reporting.emission_factors_final_calcs_latest
where emission_factor_pre != 'Forest'
	and delivery_partner = 'NS'
group by financial_year_end, delivery_partner
order by financial_year_end
), cumulative as (SELECT emissions_savings, SUM(emissions_savings) OVER (ORDER BY financial_year_end) AS cumulative_savings, financial_year_end, delivery_partner
FROM annual_emissions_savings
group by financial_year_end, delivery_partner, emissions_savings
order by financial_year_end
					   )
SELECT sum(cumulative_savings)
from cumulative;

--- annual emissions figures for full PA partnership
WITH annual_emissions_savings AS(
SELECT sum(diff_break1) as emissions_savings, financial_year_end
FROM pa_ghg_reporting.emission_factors_final_calcs_latest
where emission_factor_pre != 'Forest'
group by financial_year_end
order by financial_year_end
)

SELECT emissions_savings, SUM(emissions_savings) OVER (ORDER BY financial_year_end) AS cumulative_savings, financial_year_end
FROM annual_emissions_savings
group by financial_year_end, emissions_savings
order by financial_year_end;

--- cumulative sum total for full PA partnership
WITH annual_emissions_savings AS(
SELECT sum(diff_break1) as emissions_savings, financial_year_end
FROM pa_ghg_reporting.emission_factors_final_calcs_latest
where emission_factor_pre != 'Forest'
group by financial_year_end
order by financial_year_end
), cumulative as (
SELECT emissions_savings, SUM(emissions_savings) OVER (ORDER BY financial_year_end) AS cumulative_savings, financial_year_end
FROM annual_emissions_savings
group by financial_year_end, emissions_savings
order by financial_year_end
	)
SELECT sum(cumulative_savings)
FROM cumulative;