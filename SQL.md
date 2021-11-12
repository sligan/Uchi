### 2.1
``` sql
with t1 as(
    select st_id, timest, correct,lag(timest) over (partition by st_id) as prev
    from peas
    where correct = True
    and timest between '2020.03.01' and '2020.04.01'
    ),
t2 as(
    select *, timest - prev as active_time
    from t1
    ),
t3 as (
select count(st_id) as count_id, count(correct)
from t2
where active_time <= 180, 
having count(correct) >=20
    )
select count_id
from t3
```


### 2.2
``` sql
with checks_s as
    (select studs.test_grp, *
    from checks
    left join studs on studs.st_id = checks.st_id
    group by studs.test_grp
),
peas_s as (
    select studs.test_grp, *
    from peas
    left join studs on studs.st_id = peas.st_id
    group by studs.test_grp),
checks_math as (
    select test_grp, st_id as bought_math
    from checks_s
    where subject = 'math'
    group by test_grp
),
peas_math as (
    select test_grp, st_id as active_math
    from peas_s
    where subject = 'math'
    group by test_grp
)
select checks_s.test_grp,
    sum(checks_s.money) / count(studs.st_id) as ARPU,
    sum(checks_s.money) / count(distinct peas_s.st_id) as ARPAU,
    (count(distinct checks_s.st_id) / count(studs.st_id))*100 as CR,
    (count(distinct checks_s.st_id) / count(distinct peas_s.st_id))*100 as CR_active_users,
    (count(distinct checks_math.bought_math) / count(distinct peas_math.active_math))*100 as CR_math
from checks_s, peas_s, peas_math, checks_math
group by checks_s.test_grp
