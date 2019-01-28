with latest_nb_rating_requests as (
select * from (select rr.*,
                      row_number() over (partition by p.account_id order by rr.created_at desc)
               from server_public.rating_requests as rr
               join server_public.profile_rating_data as prd on prd.id = rr.profile_rating_data_id
               join server_public.profiles as p on p.id = prd.profile_id
               where rr.reason in ('new-business', 'rerate', 'override') --These seem right, I would be careful these are exatly the reasons you want.
    ) where row_number = 1
),


latest_renewal_rating_requests as (
select * from (select rr.*,
                      ptr.policy_term_id as current_policy_term_id,
                      renr.policy_term_revision_id as current_policy_term_revision_id,
                      ptr.quote_id as current_policy_term_revision_quote_id,
                      ptr.term_expiration_date as current_policy_term_expiration_date,
                      pt.invoice_period,
                      row_number() over (partition by ptr.policy_term_id order by renr.created_at desc)
               from server_public.rating_requests as rr
               join server_public.renewal_requests as renr on renr.renewal_rating_request_id = rr.id
               join server_public.policy_term_revisions as ptr on ptr.id = renr.policy_term_revision_id
               join server_public.policy_terms as pt on pt.id = ptr.policy_term_id
    ) where row_number = 1
),


policy_terms_with_tenure as (
select *,
       row_number() over (partition by policy_id order by created_at asc) - 1 as prior_policy_term_count
from server_public.policy_terms
),
-- Get representative bound new business quotes (i.e. quotes with an associated policy term revision ID, but not the custom tier if that is what the customer purchased)
bound_nb_quotes as (
select quote_id,
       current_policy_term_revision_quote_id,
       rate_id,
       rating_request_id,
       profile_id,
       account_id,
       decision_date,
       tier,
       tier_rank,
       market,
       renewal,
       prior_policy_term_count,
       invoice_period,
       bound,
       bound_within_35_days,
       bound_within_70_days
from (select *,
             row_number() over (partition by rating_request_id order by tier_rank)
      from (select q.id as quote_id,
                   null as current_policy_term_revision_quote_id,
                   r.id as rate_id,
                   rr.id as rating_request_id,
                   p.id as profile_id,
                   af.account_id,
                   trunc(q.created_at) as decision_date,
                   q.tier,
                   case when q.tier = 'recommended' then 1
                        when q.tier = 'low' then 2
                        when q.tier = 'high' then 3
                        when q.tier = 'custom' then 4 else null end as tier_rank,
                   r.market,
                   false as renewal,
                   0 as prior_policy_term_count,
                   pt.invoice_period as invoice_period,
                   true as bound,
                    --Not sure your intentin but metrics like these are usually quote to bind times. I also changed it to a binary so null mean they havent had the quote long enough
                   case
                    WHEN  datediff('days', q.created_at, getdate()) < 35 THEN NULL
                    when datediff('days', q.created_at, iptr.created_at) < 35 then 0
                    else 1
                  end as bound_within_35_days,
                   case
                    WHEN  datediff('days', q.created_at, getdate()) < 35 THEN NULL
                    when datediff('days', q.created_at, iptr.created_at) < 70 then 0
                    else 1
                  end as bound_within_70_days
            from server_public.quotes as q
            join server_public.rates as r on r.id = q.rate_id
            join latest_nb_rating_requests as rr on rr.rate_id = r.id
            join server_public.profiles as p on p.id = r.profile_id
            join public.account_facts as af on af.account_id = p.account_id
            left join server_public.initial_policy_term_revisions as iptr on iptr.quote_id = q.id
            left join server_public.policy_terms as pt on pt.id = iptr.policy_term_id
            where iptr.id is not null
        )
    ) where row_number = 1
),


-- Get representative unbound new business quotes (i.e. quotes with no associated policy term revision ID)
unbound_nb_quotes as (
select quote_id,
       current_policy_term_revision_quote_id,
       rate_id,
       rating_request_id,
       profile_id,
       account_id,
       decision_date,
       tier,
       tier_rank,
       market,
       renewal,
       prior_policy_term_count,
       invoice_period,
       bound,
       bound_within_35_days,
       bound_within_70_days
from (select *,
             row_number() over (partition by rating_request_id order by tier_rank)
      from (select q.id as quote_id,
                   null as current_policy_term_revision_quote_id,
                   r.id as rate_id,
                   rr.id as rating_request_id,
                   p.id as profile_id,
                   af.account_id,
                   trunc(q.created_at) as decision_date,
                   q.tier,
                   case when q.tier = 'recommended' then 1
                        when q.tier = 'low' then 2
                        when q.tier = 'high' then 3
                        when q.tier = 'custom' then 4 else null end as tier_rank,
                   r.market,
                   false as renewal,
                   0 as prior_policy_term_count,
                   'unknown' as invoice_period,
                   false as bound,
                   --Not sure your intentin but metrics like these are usually quote to bind times. I also changed it to a binary so null mean they havent had the quote long enough
                  case
                   WHEN  datediff('days', q.created_at, getdate()) < 35 THEN NULL
                   when datediff('days', q.created_at, iptr.created_at) < 35 then 0
                   else 1
                 end as bound_within_35_days,
                  case
                   WHEN  datediff('days', q.created_at, getdate()) < 35 THEN NULL
                   when datediff('days', q.created_at, iptr.created_at) < 70 then 0
                   else 1
                 end as bound_within_70_days
            from server_public.quotes as q
            join server_public.rates as r on r.id = q.rate_id
            join latest_nb_rating_requests as rr on rr.rate_id = r.id
            join server_public.profiles as p on p.id = r.profile_id
            join public.account_facts as af on af.account_id = p.account_id
            left join server_public.initial_policy_term_revisions as iptr on iptr.quote_id = q.id
            where iptr.id is null
        )
    ) where row_number = 1
),
-- Get representative bound renewal quotes (i.e. quotes with an associated policy term revision ID, but not the custom tier if that is what the customer purchased)
bound_renewal_quotes as (
select quote_id,
       current_policy_term_revision_quote_id,
       rate_id,
       rating_request_id,
       profile_id,
       account_id,
       decision_date,
       tier,
       tier_rank,
       market,
       renewal,
       prior_policy_term_count,
       invoice_period,
       bound,
       bound_within_35_days,
       bound_within_70_days
from (select *,
             row_number() over (partition by rating_request_id order by tier_rank)
      from (select q.id as quote_id,
                   rr.current_policy_term_revision_quote_id,
                   r.id as rate_id,
                   rr.id as rating_request_id,
                   p.id as profile_id,
                   af.account_id,
                   rr.current_policy_term_expiration_date as decision_date,
                   q.tier,
                   case when q.tier = 'recommended' then 1
                        when q.tier = 'low' then 2
                        when q.tier = 'high' then 3
                        when q.tier = 'custom' then 4 else null end as tier_rank,
                   r.market,
                   true as renewal,
                   pt.prior_policy_term_count + 1 as prior_policy_term_count,
                   rr.invoice_period,
                   true as bound,
                   --Not sure your intentin but metrics like these are usually quote to bind times. I also changed it to a binary so null mean they havent had the quote long enough
                   case
                    WHEN  datediff('days', rr.current_policy_term_expiration_date, getdate()) < 35 THEN NULL
                    when datediff('days', rr.current_policy_term_expiration_date, iptr.created_at) < 35 then 0
                    else 1
                  end as bound_within_35_days,
                   case
                    WHEN  datediff('days', rr.current_policy_term_expiration_date, getdate()) < 35 THEN NULL
                    when datediff('days', rr.current_policy_term_expiration_date, iptr.created_at) < 70 then 0
                    else 1
                  end as bound_within_70_days
            from server_public.quotes as q
            join server_public.rates as r on r.id = q.rate_id
            join latest_renewal_rating_requests as rr on rr.rate_id = r.id
            join policy_terms_with_tenure as pt on pt.id = rr.current_policy_term_id
            join server_public.profiles as p on p.id = r.profile_id
            join public.account_facts as af on af.account_id = p.account_id
            left join server_public.initial_policy_term_revisions as iptr on iptr.quote_id = q.id
            where iptr.id is not null
        )
    ) where row_number = 1
),


-- Get representative unbound renewal quotes
unbound_renewal_quotes as (
select quote_id,
       current_policy_term_revision_quote_id,
       rate_id,
       rating_request_id,
       profile_id,
       account_id,
       decision_date,
       tier,
       tier_rank,
       market,
       renewal,
       prior_policy_term_count,
       invoice_period,
       bound,
       bound_within_35_days,
       bound_within_70_days
from (select *,
             row_number() over (partition by rating_request_id order by tier_rank)
      from (select q.id as quote_id,
                   rr.current_policy_term_revision_quote_id,
                   r.id as rate_id,
                   rr.id as rating_request_id,
                   p.id as profile_id,
                   af.account_id,
                   rr.current_policy_term_expiration_date as decision_date,
                   q.tier,
                   case when q.tier = 'recommended' then 1
                        when q.tier = 'low' then 2
                        when q.tier = 'high' then 3
                        when q.tier = 'custom' then 4 else null end as tier_rank,
                   r.market,
                   true as renewal,
                   pt.prior_policy_term_count + 1 as prior_policy_term_count,
                   rr.invoice_period,
                   false as bound,
                   --Not sure your intentin but metrics like these are usually quote to bind times. I also changed it to a binary so null mean they havent had the quote long enough
                  case
                   WHEN  datediff('days', rr.current_policy_term_expiration_date, getdate()) < 35 THEN NULL
                   when datediff('days', rr.current_policy_term_expiration_date, iptr.created_at) < 35 then 0
                   else 1
                 end as bound_within_35_days,
                  case
                   WHEN  datediff('days', rr.current_policy_term_expiration_date, getdate()) < 35 THEN NULL
                   when datediff('days', rr.current_policy_term_expiration_date, iptr.created_at) < 70 then 0
                   else 1
                 end as bound_within_70_days
            from server_public.quotes as q
            join server_public.rates as r on r.id = q.rate_id
            join latest_renewal_rating_requests as rr on rr.rate_id = r.id
            join policy_terms_with_tenure as pt on pt.id = rr.current_policy_term_id
            join server_public.profiles as p on p.id = r.profile_id
            join public.account_facts as af on af.account_id = p.account_id
            left join server_public.initial_policy_term_revisions as iptr on iptr.quote_id = q.id
            where iptr.id is null
        )
    ) where row_number = 1
),


-- Total premium by coverage for each quote, paid-in-full and monthly, along with vehicle count
premium_info as (
select q.id as quote_id,
       sum(vq.paid_in_full_bi_premium_in_dollars)::float as paid_in_full_bi_premium_in_dollars,
       sum(vq.paid_in_full_pd_premium_in_dollars)::float as paid_in_full_pd_premium_in_dollars,
       sum(vq.paid_in_full_coll_premium_in_dollars)::float as paid_in_full_coll_premium_in_dollars,
       sum(vq.paid_in_full_comp_premium_in_dollars)::float as paid_in_full_comp_premium_in_dollars,
       sum(vq.paid_in_full_med_pay_premium_in_dollars)::float as paid_in_full_med_pay_premium_in_dollars,
       sum(vq.paid_in_full_pip_premium_in_dollars)::float as paid_in_full_pip_premium_in_dollars,
       sum(vq.paid_in_full_rental_premium_in_dollars)::float as paid_in_full_rental_premium_in_dollars,
       q.paid_in_full_umuim_premium_in_dollars::float as paid_in_full_umuim_premium_in_dollars,
       (sum(vq.paid_in_full_bi_premium_in_dollars) + sum(vq.paid_in_full_pd_premium_in_dollars) + sum(vq.paid_in_full_coll_premium_in_dollars) +
        sum(vq.paid_in_full_comp_premium_in_dollars) + sum(vq.paid_in_full_med_pay_premium_in_dollars) + sum(vq.paid_in_full_pip_premium_in_dollars) +
        sum(vq.paid_in_full_rental_premium_in_dollars) + q.paid_in_full_umuim_premium_in_dollars)::float as paid_in_full_total_premium_in_dollars,
       sum(vq.monthly_bi_premium_in_dollars)::float as monthly_bi_premium_in_dollars,
       sum(vq.monthly_pd_premium_in_dollars)::float as monthly_pd_premium_in_dollars,
       sum(vq.monthly_coll_premium_in_dollars)::float as monthly_coll_premium_in_dollars,
       sum(vq.monthly_comp_premium_in_dollars)::float as monthly_comp_premium_in_dollars,
       sum(vq.monthly_med_pay_premium_in_dollars)::float as monthly_med_pay_premium_in_dollars,
       sum(vq.monthly_pip_premium_in_dollars)::float as monthly_pip_premium_in_dollars,
       sum(vq.monthly_rental_premium_in_dollars)::float as monthly_rental_premium_in_dollars,
       q.monthly_umuim_premium_in_dollars::float as monthly_umuim_premium_in_dollars,
       (sum(vq.monthly_bi_premium_in_dollars) + sum(vq.monthly_pd_premium_in_dollars) + sum(vq.monthly_coll_premium_in_dollars) +
        sum(vq.monthly_comp_premium_in_dollars) + sum(vq.monthly_med_pay_premium_in_dollars) + sum(vq.monthly_pip_premium_in_dollars) +
        sum(vq.monthly_rental_premium_in_dollars) + q.monthly_umuim_premium_in_dollars)::float as monthly_total_premium_in_dollars,
       count(vq.vin)::float as vehicle_count
from server_public.quotes as q
left join server_public.vehicle_quotes as vq
on vq.quote_id = q.id
group by q.id, q.paid_in_full_umuim_premium_in_dollars, q.monthly_umuim_premium_in_dollars
),


-- Information on coverage options (e.g. BI/PD limits, Coll/Comp deductibles)
coverage_info as (
select q.id as quote_id,
       min(bic.per_person) as bi_limit_per_person_min,
       avg(bic.per_person) as bi_limit_per_person_avg,
       max(bic.per_person) as bi_limit_per_person_max,
       min(bic.per_occurrence) as bi_limit_per_occurrence_min,
       avg(bic.per_occurrence) as bi_limit_per_occurrence_avg,
       max(bic.per_occurrence) as bi_limit_per_occurrence_max,
       min(pdc.per_occurrence) as pd_limit_per_occurrence_min,
       avg(pdc.per_occurrence) as pd_limit_per_occurrence_avg,
       max(pdc.per_occurrence) as pd_limit_per_occurrence_max,
       min(collc.deductible) as coll_deductible_min,
       avg(collc.deductible) as coll_deductible_avg,
       max(collc.deductible) as coll_deductible_max,
       min(compc.deductible) as comp_deductible_min,
       avg(compc.deductible) as comp_deductible_avg,
       max(compc.deductible) as comp_deductible_max,
       min(med_payc.per_person) as med_pay_limit_per_person_min,
       avg(med_payc.per_person) as med_pay_limit_per_person_avg,
       max(med_payc.per_person) as med_pay_limit_per_person_max,
       min(pipc.per_person) as pip_limit_per_person_min,
       avg(pipc.per_person) as pip_limit_per_person_avg,
       max(pipc.per_person) as pip_limit_per_person_max,
       min(pipc.per_occurrence) as pip_limit_per_occurrence_min,
       avg(pipc.per_occurrence) as pip_limit_per_occurrence_avg,
       max(pipc.per_occurrence) as pip_limit_per_occurrence_max,
       min(rentalc.per_occurrence) as rental_limit_per_occurrence_min,
       avg(rentalc.per_occurrence) as rental_limit_per_occurrence_avg,
       max(rentalc.per_occurrence) as rental_limit_per_occurrence_max,
       min(rentalc.per_day) as rental_limit_per_day_min,
       avg(rentalc.per_day) as rental_limit_per_day_avg,
       max(rentalc.per_day) as rental_limit_per_day_max,
       max(umc.per_person) as um_limit_per_person,
       max(umc.per_occurrence) as um_limit_per_occurrence,
       max(uimc.per_person) as uim_limit_per_person,
       max(uimc.per_occurrence) as uim_limit_per_occurrence,
       max(umuimc.per_person) as umuim_limit_per_person,
       max(umuimc.per_occurrence) as umuim_limit_per_occurrence,
       max(umpdc.per_occurrence) as umpd_limit_per_occurrence,
       max(uimpdc.per_occurrence) as uimpd_limit_per_occurrence
from server_public.quotes as q
left join server_public.vehicle_quotes as vq on vq.quote_id = q.id
left join (select cvq.vehicle_quote_id, c.* from server_public.coverages_vehicle_quotes as cvq join server_public.coverages as c on c.id = cvq.coverage_id where c.symbol = 'bi') as bic on bic.vehicle_quote_id = vq.id
left join (select cvq.vehicle_quote_id, c.* from server_public.coverages_vehicle_quotes as cvq join server_public.coverages as c on c.id = cvq.coverage_id where c.symbol = 'pd') as pdc on pdc.vehicle_quote_id = vq.id
left join (select cvq.vehicle_quote_id, c.* from server_public.coverages_vehicle_quotes as cvq join server_public.coverages as c on c.id = cvq.coverage_id where c.symbol = 'coll') as collc on collc.vehicle_quote_id = vq.id
left join (select cvq.vehicle_quote_id, c.* from server_public.coverages_vehicle_quotes as cvq join server_public.coverages as c on c.id = cvq.coverage_id where c.symbol = 'comp') as compc on compc.vehicle_quote_id = vq.id
left join (select cvq.vehicle_quote_id, c.* from server_public.coverages_vehicle_quotes as cvq join server_public.coverages as c on c.id = cvq.coverage_id where c.symbol = 'med_pay') as med_payc on med_payc.vehicle_quote_id = vq.id
left join (select cvq.vehicle_quote_id, c.* from server_public.coverages_vehicle_quotes as cvq join server_public.coverages as c on c.id = cvq.coverage_id where c.symbol = 'pip') as pipc on pipc.vehicle_quote_id = vq.id
left join (select cvq.vehicle_quote_id, c.* from server_public.coverages_vehicle_quotes as cvq join server_public.coverages as c on c.id = cvq.coverage_id where c.symbol = 'rental') as rentalc on rentalc.vehicle_quote_id = vq.id
left join (select cq.quote_id, c.* from server_public.coverages_quotes as cq join server_public.coverages as c on c.id = cq.coverage_id where c.symbol = 'um') as umc on umc.quote_id = q.id
left join (select cq.quote_id, c.* from server_public.coverages_quotes as cq join server_public.coverages as c on c.id = cq.coverage_id where c.symbol = 'uim') as uimc on uimc.quote_id = q.id
left join (select cq.quote_id, c.* from server_public.coverages_quotes as cq join server_public.coverages as c on c.id = cq.coverage_id where c.symbol = 'umuim') as umuimc on umuimc.quote_id = q.id
left join (select cq.quote_id, c.* from server_public.coverages_quotes as cq join server_public.coverages as c on c.id = cq.coverage_id where c.symbol = 'umpd') as umpdc on umpdc.quote_id = q.id
left join (select cq.quote_id, c.* from server_public.coverages_quotes as cq join server_public.coverages as c on c.id = cq.coverage_id where c.symbol = 'uimpd') as uimpdc on uimpdc.quote_id = q.id
group by q.id
),


-- Hypothetical collision premium at a $500 deductible and with all vehicles having full coverage
-- Used for the physdam conversion/renewal models where the customer bound but declined physical damage coverage
-- Assume a $500 deductible to get hypothetical premium (row_number = 2)
hypothetical_coll_premium as (
select * from (
               select *,
                      row_number() over (partition by quote_id order by coll_deductible_hypothetical desc)
               from (
                     select q.id as quote_id,
                            c.deductible as coll_deductible_hypothetical,
                            sum(vrc.all_full_coverage_premium_dollars) as monthly_coll_premium_in_dollars_hypothetical,
                            sum(vrc.all_full_coverage_premium_dollars_with_paid_in_full_discount) as paid_in_full_coll_premium_in_dollars_hypothetical
                     from server_public.quotes as q
                     join server_public.rates as r on r.id = q.rate_id
                     join server_public.vehicle_rates as vr on vr.rate_id = r.id
                     join server_public.vehicle_rate_coverages as vrc on vrc.vehicle_rate_id = vr.id
                     join server_public.coverages as c on c.id = vrc.coverage_id
                     where c.symbol = 'coll' and
                           c.deductible is not null
                     group by q.id, c.deductible
        )
    ) where row_number = 2
),


-- Hypothetical comp premium at a $500 deductible and with all vehicles having full coverage
-- Used for the physdam conversion/renewal models where the customer bound but declined physical damage coverage
-- Assume a $500 deductible to get hypothetical premium (row_number = 2)
hypothetical_comp_premium as (
select * from (
               select *,
                      row_number() over (partition by quote_id order by comp_deductible_hypothetical desc)
               from (
                     select q.id as quote_id,
                            c.deductible as comp_deductible_hypothetical,
                            sum(vrc.all_full_coverage_premium_dollars) as monthly_comp_premium_in_dollars_hypothetical,
                            sum(vrc.all_full_coverage_premium_dollars_with_paid_in_full_discount) as paid_in_full_comp_premium_in_dollars_hypothetical
                     from server_public.quotes as q
                     join server_public.rates as r on r.id = q.rate_id
                     join server_public.vehicle_rates as vr on vr.rate_id = r.id
                     join server_public.vehicle_rate_coverages as vrc on vrc.vehicle_rate_id = vr.id
                     join server_public.coverages as c on c.id = vrc.coverage_id
                     where c.symbol = 'comp' and
                           c.deductible is not null
                     group by q.id, c.deductible
        )
    ) where row_number = 2
),


-- Total on-level non-UBI premium by coverage for each quote for use as a stand-in for competitor premium
non_ubi_rerate_quotes as (
select * from (
               select rq.id,
                      rq.rerate_id,
                      rq.monthly_umuim_premium_in_dollars,
                      row_number() over (partition by r.id order by rr.version::int desc)
               from rerating_public.rerates as r
               join rerating_public.rerate_quotes as rq on rq.rerate_id = r.id
               join rerating_public.rerate_rates as rr on rr.id = rq.rerate_rate_id
               where r.reason = 'non_ubi_premium_analysis' and
                     r.analysis_version = 1
    ) where row_number = 1
),


non_ubi_premium_info as (
select q.id as quote_id,
       sum(rvq.monthly_bi_premium_in_dollars)::float as monthly_bi_premium_in_dollars_nonubi,
       sum(rvq.monthly_pd_premium_in_dollars)::float as monthly_pd_premium_in_dollars_nonubi,
       sum(rvq.monthly_coll_premium_in_dollars)::float as monthly_coll_premium_in_dollars_nonubi,
       sum(rvq.monthly_comp_premium_in_dollars)::float as monthly_comp_premium_in_dollars_nonubi,
       sum(rvq.monthly_med_pay_premium_in_dollars)::float as monthly_med_pay_premium_in_dollars_nonubi,
       sum(rvq.monthly_pip_premium_in_dollars)::float as monthly_pip_premium_in_dollars_nonubi,
       sum(rvq.monthly_rental_premium_in_dollars)::float as monthly_rental_premium_in_dollars_nonubi,
       rq.monthly_umuim_premium_in_dollars::float as monthly_umuim_premium_in_dollars_nonubi,
       (sum(rvq.monthly_bi_premium_in_dollars) + sum(rvq.monthly_pd_premium_in_dollars) + sum(rvq.monthly_coll_premium_in_dollars) +
        sum(rvq.monthly_comp_premium_in_dollars) + sum(rvq.monthly_med_pay_premium_in_dollars) + sum(rvq.monthly_pip_premium_in_dollars) +
        sum(rvq.monthly_rental_premium_in_dollars) + rq.monthly_umuim_premium_in_dollars)::float as monthly_total_premium_in_dollars_nonubi
from rerating_public.rerates as r
join non_ubi_rerate_quotes as rq on rq.rerate_id = r.id
join rerating_public.rerate_vehicle_quotes as rvq on rvq.rerate_quote_id = rq.id
join server_public.quotes as q on q.id = r.quote_id
group by q.id, rq.monthly_umuim_premium_in_dollars
),


-- Get daveability and distracted driving scores to estimate the "competitor" ratios where non-UBI premium is unavailable
ubi_scores as (
select prd.id as profile_rating_data_id,
       avg(case when drd.daveability_score = 'no_use' or drd.daveability_score = 'test_drive' then null else drd.daveability_score::float end) as mean_daveability_score,
       avg(case when drd.distracted_driving_score = 'no_use' or drd.distracted_driving_score = 'test_drive' then null else drd.distracted_driving_score::float end) as mean_distracted_driving_score
from server_public.profile_rating_data as prd
join server_public.driver_rating_data as drd on drd.profile_rating_data_id = prd.id
group by prd.id
order by prd.id
)


-- Put everything together
select base.*,
       pi.vehicle_count,

       -- Coverage options; currently these are the same across all vehicles so the max is taken, but we need to remain aware that that might change in the future
       ci.bi_limit_per_person_max as bi_limit_per_person,
       ci.bi_limit_per_occurrence_max as bi_limit_per_occurrence,
       ci.pd_limit_per_occurrence_max as pd_limit_per_occurrence,
       ci.coll_deductible_max as coll_deductible,
       ci.comp_deductible_max as comp_deductible,
       ci.med_pay_limit_per_person_max as med_pay_limit_per_person,
       ci.pip_limit_per_person_max as pip_limit_per_person,
       ci.pip_limit_per_occurrence_max as pip_limit_per_occurrence,
       ci.rental_limit_per_day_max as rental_limit_per_day,
       ci.rental_limit_per_occurrence_max as rental_limit_per_occurrence,
       ci.um_limit_per_person as um_limit_per_person,
       ci.um_limit_per_occurrence as um_limit_per_occurrence,
       ci.uim_limit_per_person as uim_limit_per_person,
       ci.uim_limit_per_occurrence as uim_limit_per_occurrence,
       ci.umuim_limit_per_person as umuim_limit_per_person,
       ci.umuim_limit_per_occurrence as umuim_limit_per_occurrence,
       ci.umpd_limit_per_occurrence as umpd_limit_per_occurrence,
       ci.uimpd_limit_per_occurrence as uimpd_limit_per_occurrence,
       coll_hypothetical.coll_deductible_hypothetical as coll_deductible_hypothetical,
       comp_hypothetical.comp_deductible_hypothetical as comp_deductible_hypothetical,
       case when pi_current.monthly_coll_premium_in_dollars + pi_current.monthly_comp_premium_in_dollars > 0 then 1
            when pi_current.monthly_coll_premium_in_dollars + pi_current.monthly_comp_premium_in_dollars = 0 then 0
            else null end as coll_comp_on_current_policy,

       -- Quoted premium, which depends on invoice period; for unbound new business we don\'t know what invoice period they were considering, so we make an assumption based on our existing invoice period mix and take a weighted average
       case when base.invoice_period = 'monthly' then pi.monthly_bi_premium_in_dollars + pi.monthly_pd_premium_in_dollars
            when base.invoice_period = 'full_term' then pi.paid_in_full_bi_premium_in_dollars + pi.paid_in_full_pd_premium_in_dollars
            else .3 * (pi.paid_in_full_bi_premium_in_dollars + pi.paid_in_full_pd_premium_in_dollars) + .7 * (pi.monthly_bi_premium_in_dollars + pi.monthly_pd_premium_in_dollars) end as bi_pd_quoted_premium,
       case when base.invoice_period = 'monthly' then pi.monthly_coll_premium_in_dollars + pi.monthly_comp_premium_in_dollars
            when base.invoice_period = 'full_term' then pi.paid_in_full_coll_premium_in_dollars + pi.paid_in_full_comp_premium_in_dollars
            else .3 * (pi.paid_in_full_coll_premium_in_dollars + pi.paid_in_full_comp_premium_in_dollars) + .7 * (pi.monthly_coll_premium_in_dollars + pi.monthly_comp_premium_in_dollars) end as coll_comp_quoted_premium,
       case when base.invoice_period = 'monthly' then pi.monthly_med_pay_premium_in_dollars
            when base.invoice_period = 'full_term' then pi.paid_in_full_med_pay_premium_in_dollars
            else .3 * pi.paid_in_full_med_pay_premium_in_dollars + .7 * pi.monthly_med_pay_premium_in_dollars end as med_pay_quoted_premium,
       case when base.invoice_period = 'monthly' then pi.monthly_pip_premium_in_dollars
            when base.invoice_period = 'full_term' then pi.paid_in_full_pip_premium_in_dollars
            else .3 * pi.paid_in_full_pip_premium_in_dollars + .7 * pi.monthly_pip_premium_in_dollars end as pip_quoted_premium,
       case when base.invoice_period = 'monthly' then pi.monthly_rental_premium_in_dollars
            when base.invoice_period = 'full_term' then pi.paid_in_full_rental_premium_in_dollars
            else .3 * pi.paid_in_full_rental_premium_in_dollars + .7 * pi.monthly_rental_premium_in_dollars end as rental_quoted_premium,
       case when base.invoice_period = 'monthly' then pi.monthly_umuim_premium_in_dollars
            when base.invoice_period = 'full_term' then pi.paid_in_full_umuim_premium_in_dollars
            else .3 * pi.paid_in_full_umuim_premium_in_dollars + .7 * pi.monthly_umuim_premium_in_dollars end as umuim_quoted_premium,
       case when base.invoice_period = 'monthly' then pi.monthly_total_premium_in_dollars
            when base.invoice_period = 'full_term' then pi.paid_in_full_total_premium_in_dollars
            else .3 * pi.paid_in_full_total_premium_in_dollars + .7 * pi.monthly_total_premium_in_dollars end as total_quoted_premium,

       -- Hypothetical premium, which also depends on invoice period; used for modeling optional coverages in situations where the customer declined and actual quoted premium is 0
       case when base.invoice_period = 'monthly' then coll_hypothetical.monthly_coll_premium_in_dollars_hypothetical + comp_hypothetical.monthly_comp_premium_in_dollars_hypothetical
            when base.invoice_period = 'full_term' then coll_hypothetical.paid_in_full_coll_premium_in_dollars_hypothetical + comp_hypothetical.paid_in_full_comp_premium_in_dollars_hypothetical
            else .3 * (coll_hypothetical.paid_in_full_coll_premium_in_dollars_hypothetical + comp_hypothetical.paid_in_full_comp_premium_in_dollars_hypothetical) + .7 * (coll_hypothetical.monthly_coll_premium_in_dollars_hypothetical + comp_hypothetical.monthly_comp_premium_in_dollars_hypothetical) end as coll_comp_quoted_premium_hypothetical,

       -- Quoted car years for each coverage
       .5 * pi.vehicle_count as bi_pd_quoted_car_years,
       case when pi.paid_in_full_coll_premium_in_dollars = 0 and pi.paid_in_full_comp_premium_in_dollars = 0 then 0 else .5 * pi.vehicle_count end as coll_comp_quoted_car_years,
       case when pi.paid_in_full_med_pay_premium_in_dollars = 0 then 0 else .5 * pi.vehicle_count end as med_pay_quoted_car_years,
       case when pi.paid_in_full_pip_premium_in_dollars = 0 then 0 else .5 * pi.vehicle_count end as pip_quoted_car_years,
       case when pi.paid_in_full_rental_premium_in_dollars = 0 then 0 else .5 * pi.vehicle_count end as rental_quoted_car_years,
       case when pi.paid_in_full_umuim_premium_in_dollars = 0 then 0 else .5 * pi.vehicle_count end as umuim_quoted_car_years,

       -- Root-to-competitor premium ratio calculations
       case when (nupi.monthly_bi_premium_in_dollars_nonubi + nupi.monthly_pd_premium_in_dollars_nonubi) = 0 or (nupi.monthly_bi_premium_in_dollars_nonubi + nupi.monthly_pd_premium_in_dollars_nonubi) is null then null else (pi.monthly_bi_premium_in_dollars + pi.monthly_pd_premium_in_dollars) / (nupi.monthly_bi_premium_in_dollars_nonubi + nupi.monthly_pd_premium_in_dollars_nonubi) end as bi_pd_root_to_competitor_ratio,
       case when (nupi.monthly_coll_premium_in_dollars_nonubi + nupi.monthly_comp_premium_in_dollars_nonubi) = 0 or (nupi.monthly_coll_premium_in_dollars_nonubi + nupi.monthly_comp_premium_in_dollars_nonubi) is null then null else (pi.monthly_coll_premium_in_dollars + pi.monthly_comp_premium_in_dollars) / (nupi.monthly_coll_premium_in_dollars_nonubi + nupi.monthly_comp_premium_in_dollars_nonubi) end as coll_comp_root_to_competitor_ratio,
       case when nupi.monthly_med_pay_premium_in_dollars_nonubi = 0 or nupi.monthly_med_pay_premium_in_dollars_nonubi is null then null else pi.monthly_med_pay_premium_in_dollars / nupi.monthly_med_pay_premium_in_dollars_nonubi end as med_pay_root_to_competitor_ratio,
       case when nupi.monthly_pip_premium_in_dollars_nonubi = 0 or nupi.monthly_pip_premium_in_dollars_nonubi is null then null else pi.monthly_pip_premium_in_dollars / nupi.monthly_pip_premium_in_dollars_nonubi end as pip_root_to_competitor_ratio,
       case when nupi.monthly_rental_premium_in_dollars_nonubi = 0 or nupi.monthly_rental_premium_in_dollars_nonubi is null then null else pi.monthly_rental_premium_in_dollars / nupi.monthly_rental_premium_in_dollars_nonubi end as rental_root_to_competitor_ratio,
       case when nupi.monthly_umuim_premium_in_dollars_nonubi = 0 or nupi.monthly_umuim_premium_in_dollars_nonubi is null then null else pi.monthly_umuim_premium_in_dollars / nupi.monthly_umuim_premium_in_dollars_nonubi end as umuim_root_to_competitor_ratio,
       case when nupi.monthly_total_premium_in_dollars_nonubi = 0 or nupi.monthly_total_premium_in_dollars_nonubi is null then null else pi.monthly_total_premium_in_dollars / nupi.monthly_total_premium_in_dollars_nonubi end as total_root_to_competitor_ratio,

       -- Renewal rate change calculations
       case when (pi_current.monthly_bi_premium_in_dollars + pi_current.monthly_pd_premium_in_dollars) = 0 or (pi_current.monthly_bi_premium_in_dollars + pi_current.monthly_pd_premium_in_dollars) is null then null
            else ((pi.monthly_bi_premium_in_dollars + pi.monthly_pd_premium_in_dollars) / pi.vehicle_count) / ((pi_current.monthly_bi_premium_in_dollars + pi_current.monthly_pd_premium_in_dollars) / pi_current.vehicle_count) - 1 end as bi_pd_renewal_rate_change,
       case when (pi_current.monthly_coll_premium_in_dollars + pi_current.monthly_comp_premium_in_dollars) = 0 or (pi_current.monthly_coll_premium_in_dollars + pi_current.monthly_comp_premium_in_dollars) is null then null
            else ((pi.monthly_coll_premium_in_dollars + pi.monthly_comp_premium_in_dollars) / pi.vehicle_count) / ((pi_current.monthly_coll_premium_in_dollars + pi_current.monthly_comp_premium_in_dollars) / pi_current.vehicle_count) - 1 end as coll_comp_renewal_rate_change,
       case when pi_current.monthly_med_pay_premium_in_dollars = 0 or pi_current.monthly_med_pay_premium_in_dollars is null then null else pi.monthly_med_pay_premium_in_dollars / pi_current.monthly_med_pay_premium_in_dollars - 1 end as med_pay_renewal_rate_change,
       case when pi_current.monthly_pip_premium_in_dollars = 0 or pi_current.monthly_pip_premium_in_dollars is null then null else pi.monthly_pip_premium_in_dollars / pi_current.monthly_pip_premium_in_dollars - 1 end as pip_renewal_rate_change,
       case when pi_current.monthly_rental_premium_in_dollars = 0 or pi_current.monthly_rental_premium_in_dollars is null then null else pi.monthly_rental_premium_in_dollars / pi_current.monthly_rental_premium_in_dollars - 1 end as rental_renewal_rate_change,
       case when pi_current.monthly_umuim_premium_in_dollars = 0 or pi_current.monthly_umuim_premium_in_dollars is null then null else pi.monthly_umuim_premium_in_dollars / pi_current.monthly_umuim_premium_in_dollars - 1 end as umuim_renewal_rate_change,
       case when pi_current.monthly_total_premium_in_dollars = 0 or pi_current.monthly_total_premium_in_dollars is null then null else pi.monthly_total_premium_in_dollars / pi_current.monthly_total_premium_in_dollars - 1 end as total_renewal_rate_change,
       case when (pi_current.monthly_coll_premium_in_dollars + pi_current.monthly_comp_premium_in_dollars) = 0 or (pi_current.monthly_coll_premium_in_dollars + pi_current.monthly_comp_premium_in_dollars) is null then null
            else ((coll_hypothetical.monthly_coll_premium_in_dollars_hypothetical + comp_hypothetical.monthly_comp_premium_in_dollars_hypothetical) / pi.vehicle_count) / ((pi_current.monthly_coll_premium_in_dollars + pi_current.monthly_comp_premium_in_dollars) / pi_current.vehicle_count) - 1 end as coll_comp_renewal_rate_change_hypothetical,

       -- Variables from account_facts
       af.day_zero,
       af.hybrid,
       af.referred,
       af.mobile_platform,
       af.pni_age,
       af.pni_gender,
       af.pni_marital_status,

       -- Variables from profile_rating_data
       case when prd.fico_insurance_score in ('no_score', 'thin_file', '0') then prd.fico_insurance_score else 'populated' end as fico_insurance_score_status,
       case when prd.fico_insurance_score in ('no_score', 'thin_file', '0') then null else prd.fico_insurance_score::float end as fico_insurance_score,
       prd.homeowner,
       case when prd.months_continuous_insurance = 'no_use' then 'no_use' else 'populated' end as months_continuous_insurance_status,
       case when prd.months_continuous_insurance = 'no_use' then null else prd.months_continuous_insurance::float end as months_continuous_insurance,
       case when prd.months_with_previous_carrier = 'no_use' then 'no_use' else 'populated' end as months_with_previous_carrier_status,
       case when prd.months_with_previous_carrier = 'no_use' then null else prd.months_with_previous_carrier::float end as months_with_previous_carrier,
       case when prd.prior_insurance_class = 'A' then '0 days without'
            when prd.prior_insurance_class = 'B' then '1-31 days without'
            when prd.prior_insurance_class = 'C' then '>31 days without or no prior'
            when prd.prior_insurance_class = 'N' then 'unknown' else null end as prior_insurance_class,
       rr.reason as rating_reason,
       ubi.mean_daveability_score,
       ubi.mean_distracted_driving_score
from (select * from bound_nb_quotes union select * from bound_renewal_quotes union select * from unbound_nb_quotes union select * from unbound_renewal_quotes) as base
left join premium_info as pi on pi.quote_id = base.quote_id
left join premium_info as pi_current on pi_current.quote_id = base.current_policy_term_revision_quote_id
left join hypothetical_coll_premium as coll_hypothetical on coll_hypothetical.quote_id = base.quote_id
left join hypothetical_comp_premium as comp_hypothetical on comp_hypothetical.quote_id = base.quote_id
left join non_ubi_premium_info as nupi on nupi.quote_id = base.quote_id
left join coverage_info as ci on ci.quote_id = base.quote_id
left join (select * from (select *, row_number() over (partition by account_id order by rate_timestamp desc) from public.account_facts) where row_number = 1) as af on af.account_id = base.account_id
left join (select * from (select *, row_number() over (partition by profile_id order by updated_at desc) from server_public.profile_rating_data) where row_number = 1) as prd on prd.profile_id = base.profile_id
left join ubi_scores as ubi on ubi.profile_rating_data_id = prd.id
left join server_public.rating_requests as rr on rr.rate_id = base.rate_id
where base.decision_date between '2018-08-28' and '2018-12-31'
;
