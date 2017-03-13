
select d1.provider_id as provider_id, d1.care_score as care_score,
d2.survey_score as survey_score 
from
	(
	select p.provider_id as provider_id, sum(p.total_score) as care_score 
	from (
	select provider_id, condition_id ,
	sum(score) as total_score,
	sum(if(score <0,1,0)) as nan_count
	from tbl_effective_care2 
	group by provider_id, condition_id
	having nan_count == 0
	) p
	group by p.provider_id
	having count(p.condition_id) >= 7
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
from tbl_surveys_responses  
) d2
on d1.provider_id == d2.provider_id
order by care_score desc
limit 100;


select d1.provider_id as provider_id, d1.care_score_var as care_score_var,
d2.survey_score as survey_score 
from
	(
	select p.provider_id as provider_id, stddev_pop(p.total_score) as care_score_var 
	from (
	select provider_id, condition_id ,
	sum(score) as total_score,
	sum(if(score <0,1,0)) as nan_count
	from tbl_effective_care2 
	group by provider_id, condition_id
	having nan_count == 0
	) p
	group by p.provider_id
	having count(p.condition_id) >= 7
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
from tbl_surveys_responses 
) d2
on d1.provider_id == d2.provider_id
order by care_score_var desc
limit 100;





