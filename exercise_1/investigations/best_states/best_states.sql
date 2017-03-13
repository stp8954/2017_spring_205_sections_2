
select h.provider_id , h.hospital_name
from
(
	select d12.provider_id as provider_id, 
	(0.5 * survey_score + 0.3 * care_score + 0.2 * readmission_score) as total_score
	from
	(
		select d1.provider_id as provider_id, d1.care_score as care_score,
		d2.survey_score as survey_score 
		from
		 (
			select p.provider_id as provider_id, sum(p.total_score) as care_score 
			from (
			select provider_id, condition_id ,
			sum(score) as total_score,
			sum(if(score <0,1,0)) as nan_count
			from tbl_effective_care 
			group by provider_id, condition_id
			having nan_count == 0
			) p
			group by p.provider_id
			having count(p.condition_id) >= 7
			order by care_score desc
		) d1
		join 
		(
			select provider_id , 
			(cwn_achivement + cwn_improvement + cwn_dimension + cwd_achievement 
				+ cwd_improvement + cwd_dimension + rhs_achievement + rhs_improvement 
				+ rhs_dimension + pm_achievement + pm_improvement + pm_dimension 
				+ cm_achievement + cm_improvement + cm_dimension + cqhe_achievement 
				+ cqhe_improvement + cqhe_dimension + di_achievement + di_improvement 
				+ di_dimension + orh_achievement + orh_improvement + orh_dimension 
				+ hcahps_base_score + hcahps_consistency_score ) as survey_score 
		from tbl_surveys_responses order by survey_score desc 
		) d2
		on d1.provider_id == d2.provider_id
	)d12
	join 
	(
		select p.provider_id as provider_id,
		count(p.measure_id) as total_measures,
		(sum(p.score) + sum(p.compared_to_national)) as readmission_score
		from 
		(select provider_id, measure_id, score, compared_to_national from tbl_readmissions
		where score >= 0) p
		group by p.provider_id
		having total_measures > 4
		order by readmission_score desc
	)d3
	on d12.provider_id == d3.provider_id 
	order by total_score desc
	limit 10
) d123
join 
(
	select provider_id, hospital_name from tbl_hospitals
)h
on d123.provider_id == h.provider_id
;

