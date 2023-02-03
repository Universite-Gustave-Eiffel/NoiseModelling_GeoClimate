-- Calcul ERPS nico
SET @UUEID='${UUEID}';

-----------------------------------------------------------------------------
-- Receiver Exposition for LDEN values
drop table if exists receiver_expo;
create table receiver_expo as SELECT PK_1, LAEQ, BUILD_PK, PERCENT_RANK() OVER (PARTITION BY BUILD_PK ORDER BY LAEQ DESC, PK_1) RECEIVER_RANK  FROM LDEN_RAILWAY L, RECEIVERS_UUEID RU, RECEIVERS_BUILDING RB WHERE RCV_TYPE = 1 AND L.IDRECEIVER = RU.PK AND PK_1 = RB.PK order by BUILD_PK, LAEQ DESC;
-- remove receivers with noise level inferior than median noise level for the same building
DELETE FROM receiver_expo WHERE RECEIVER_RANK > 0.5;
create index receiver_expo_BUILD_PK on receiver_expo(BUILD_PK);
-- divide the building population number by the number of retained receivers for each buildings
ALTER TABLE receiver_expo ADD COLUMN POP double USING (SELECT B.POP / (SELECT COUNT(*) FROM receiver_expo re WHERE re.BUILD_PK = receiver_expo.BUILD_PK) FROM BUILDINGS_SCREENS B WHERE B.PK = BUILD_PK);
-- fetch AGGLO info
ALTER TABLE receiver_expo ADD COLUMN AGGLO BOOLEAN USING (SELECT AGGLO FROM BUILDINGS_SCREENS B WHERE B.PK = BUILD_PK);
CREATE INDEX receiver_expo_laeq on receiver_expo(LAEQ);
-- update exposure table
UPDATE POPULATION_EXPOSURE SET POP_ACCURATE = COALESCE((SELECT SUM(POP) popsum FROM receiver_expo r WHERE r.LAEQ > POPULATION_EXPOSURE.MIN_LAEQ
                                                                            AND r.LAEQ < POPULATION_EXPOSURE.MAX_LAEQ  AND (NOT r.AGGLO OR exposureType = 'mostExposedFacadeIncludingAgglomeration')), POP_ACCURATE) WHERE UUEID = @UUEID AND POPULATION_EXPOSURE.NOISELEVEL LIKE 'Lden%';

-----------------------------------------------------------------------------
-- Population Receiver Exposition for LN values
drop table if exists receiver_expo;
create table receiver_expo as SELECT PK_1, LAEQ, BUILD_PK, PERCENT_RANK() OVER (PARTITION BY BUILD_PK ORDER BY LAEQ DESC, PK_1) RECEIVER_RANK  FROM LNIGHT_RAILWAY L, RECEIVERS_UUEID RU, RECEIVERS_BUILDING RB WHERE RCV_TYPE = 1 AND L.IDRECEIVER = RU.PK AND PK_1 = RB.PK order by BUILD_PK, LAEQ DESC;
-- remove receivers with noise level inferior than median noise level for the same building
DELETE FROM receiver_expo WHERE RECEIVER_RANK > 0.5;
create index receiver_expo_BUILD_PK on receiver_expo(BUILD_PK);
-- divide the building population number by the number of retained receivers for each buildings
ALTER TABLE receiver_expo ADD COLUMN POP double USING (SELECT B.POP / (SELECT COUNT(*) FROM receiver_expo re WHERE re.BUILD_PK = receiver_expo.BUILD_PK) FROM BUILDINGS_SCREENS B WHERE B.PK = BUILD_PK);
-- fetch AGGLO info
ALTER TABLE receiver_expo ADD COLUMN AGGLO BOOLEAN USING (SELECT AGGLO FROM BUILDINGS_SCREENS B WHERE B.PK = BUILD_PK);
CREATE INDEX receiver_expo_laeq on receiver_expo(LAEQ);
-- update exposure table
UPDATE POPULATION_EXPOSURE SET
POP_ACCURATE = COALESCE((SELECT SUM(POP) popsum FROM receiver_expo r WHERE r.LAEQ > POPULATION_EXPOSURE.MIN_LAEQ
                                                                            AND r.LAEQ < POPULATION_EXPOSURE.MAX_LAEQ AND (NOT r.AGGLO OR exposureType = 'mostExposedFacadeIncludingAgglomeration')), POP_ACCURATE) WHERE UUEID = @UUEID AND POPULATION_EXPOSURE.NOISELEVEL LIKE 'Lnight%';

----------------------------------------------------------------
-- Autocomplete missing values

UPDATE POPULATION_EXPOSURE SET HSD = COALESCE(ROUND(POP_ACCURATE*(19.4312-0.9336*INTERVAL_LAEQ+0.0126*INTERVAL_LAEQ*INTERVAL_LAEQ)/100.0, 1), HSD)
   WHERE UUEID = @UUEID AND NOISELEVEL LIKE CONCAT('Lnight', '%') AND POP_ACCURATE > 0;
UPDATE POPULATION_EXPOSURE B SET CPI = COALESCE(ROUND((POP_ACCURATE*((EXP((LN(1.08)/10)*(INTERVAL_LAEQ-53)))-1)/(b.POP_ACCURATE*((EXP((LN(1.08)/10)*(INTERVAL_LAEQ-53)))-1)+1))*0.00138*(SELECT SUM(a.POP_ACCURATE) from  POPULATION_EXPOSURE a WHERE a.UUEID = b.UUEID AND a.NOISELEVEL LIKE CONCAT('Lden', '%') AND A.POP_ACCURATE > 0)), CPI) WHERE UUEID = @UUEID AND NOISELEVEL LIKE CONCAT('Lden', '%') AND POP_ACCURATE > 0;
UPDATE POPULATION_EXPOSURE B SET HA = COALESCE(ROUND(POP_ACCURATE*(78.9270-3.1162*INTERVAL_LAEQ+0.0342*INTERVAL_LAEQ*INTERVAL_LAEQ)/100), HA) WHERE UUEID = @UUEID AND NOISELEVEL LIKE 'Lden%' AND POP_ACCURATE > 0;
UPDATE POPULATION_EXPOSURE B SET EXPOSEDPEOPLE = ROUND(POP_ACCURATE) WHERE UUEID = @UUEID;
UPDATE POPULATION_EXPOSURE B SET EXPOSEDDWELLINGS = COALESCE(ROUND(POP_ACCURATE / (SELECT RatIO_POP_LOG  FROM METADATA)), EXPOSEDDWELLINGS) WHERE UUEID = @UUEID;



-----------------------------------------------------------------------------
-- Counting of EXPOSED structures
DROP TABLE IF EXISTS BUILDINGS_MAX_ERPS;
-- Add new column for conversion LAEQ double to NOISELEVEL varchar
CREATE TABLE BUILDINGS_MAX_ERPS(ID_ERPS VARCHAR,ERPS_NATUR VARCHAR, AGGLO BOOLEAN, PERIOD VARCHAR(4), LAEQ DECIMAL(5,2));

INSERT INTO BUILDINGS_MAX_ERPS SELECT id_erps, ERPS_NATUR, B.AGGLO, 'LDEN' PERIOD, max(LAEQ) - 3 as LAEQ FROM  BUILDINGS_ERPS BR
                                                                                   INNER JOIN  BUILDINGS_SCREENS B ON (BR.ID_BAT = B.ID_BAT AND BR.ERPS_NATUR IN ('Enseignement', 'Sante'))
                                                                                    INNER JOIN RECEIVERS_BUILDING RB ON (B.PK = RB.BUILD_PK)
                                                                                     INNER JOIN RECEIVERS_UUEID  RU ON (RB.PK = RU.PK_1 AND RU.RCV_TYPE = 1)
                                                                                     INNER JOIN LDEN_RAILWAY LR ON (RU.PK = LR.IDRECEIVER) GROUP BY id_erps, ERPS_NATUR, B.AGGLO;

INSERT INTO BUILDINGS_MAX_ERPS SELECT id_erps, ERPS_NATUR, B.AGGLO, 'LN' PERIOD, max(LAEQ) - 3 as LAEQ FROM  BUILDINGS_ERPS BR
                                                                                   INNER JOIN  BUILDINGS_SCREENS B ON (BR.ID_BAT = B.ID_BAT AND BR.ERPS_NATUR IN ('Enseignement', 'Sante'))
                                                                                    INNER JOIN RECEIVERS_BUILDING RB ON (B.PK = RB.BUILD_PK)
                                                                                     INNER JOIN RECEIVERS_UUEID  RU ON (RB.PK = RU.PK_1 AND RU.RCV_TYPE = 1)
                                                                                     INNER JOIN LNIGHT_RAILWAY LR ON (RU.PK = LR.IDRECEIVER) GROUP BY id_erps, ERPS_NATUR, B.AGGLO;

UPDATE POPULATION_EXPOSURE SET
 EXPOSEDHOSPITALS = COALESCE((SELECT COUNT(*) FROM BUILDINGS_MAX_ERPS R  WHERE r.LAEQ > POPULATION_EXPOSURE.MIN_LAEQ
 AND r.LAEQ < POPULATION_EXPOSURE.MAX_LAEQ AND (NOT r.AGGLO OR exposureType = 'mostExposedFacadeIncludingAgglomeration')
 AND ERPS_NATUR='Sante'), EXPOSEDHOSPITALS),
  EXPOSEDSCHOOLS   = COALESCE((SELECT COUNT(*) FROM BUILDINGS_MAX_ERPS R  WHERE
  r.LAEQ > POPULATION_EXPOSURE.MIN_LAEQ
 AND r.LAEQ < POPULATION_EXPOSURE.MAX_LAEQ AND (NOT r.AGGLO OR exposureType = 'mostExposedFacadeIncludingAgglomeration')
   AND ERPS_NATUR='Enseignement'), EXPOSEDSCHOOLS) WHERE UUEID = @UUEID;

-- Sum rows to special rows like lden55
-- noise_level_class table explain where to fetch the values
drop table if exists noise_level_class;
create table noise_level_class(exposureNoiseLevel varchar, noiseLevel varchar);
insert into noise_level_class values ('Lden55', 'Lden5559'), ('Lden55', 'Lden6064'), ('Lden55', 'Lden6569'), ('Lden55', 'Lden7074'),  ('Lden55', 'LdenGreaterThan75'),
 ('Lden65', 'Lden6569'), ('Lden65', 'Lden7074'),  ('Lden65', 'LdenGreaterThan75') ,
('Lden75', 'LdenGreaterThan75');

UPDATE POPULATION_EXPOSURE B SET EXPOSEDPEOPLE = COALESCE((SELECT sum(P.EXPOSEDPEOPLE) FROM POPULATION_EXPOSURE P,
 noise_level_class C where P.NOISELEVEL = C.NOISELEVEL and C.EXPOSURENOISELEVEL=B.NOISELEVEL
 and EXPOSURETYPE  = B.EXPOSURETYPE and UUEID = @UUEID),
  EXPOSEDPEOPLE) WHERE UUEID = @UUEID;

UPDATE POPULATION_EXPOSURE B SET EXPOSEDAREA = COALESCE((SELECT sum(P.EXPOSEDAREA) FROM POPULATION_EXPOSURE P,
 noise_level_class C where P.NOISELEVEL = C.NOISELEVEL and C.EXPOSURENOISELEVEL=B.NOISELEVEL
 and EXPOSURETYPE  = B.EXPOSURETYPE and UUEID = @UUEID), EXPOSEDAREA) WHERE UUEID = @UUEID AND EXPOSURETYPE = 'mostExposedFacadeIncludingAgglomeration';

UPDATE POPULATION_EXPOSURE B SET EXPOSEDDWELLINGS = COALESCE((SELECT sum(P.EXPOSEDDWELLINGS) FROM POPULATION_EXPOSURE P,
  noise_level_class C where P.NOISELEVEL = C.NOISELEVEL and C.EXPOSURENOISELEVEL=B.NOISELEVEL
  and EXPOSURETYPE  = B.EXPOSURETYPE and UUEID = @UUEID), EXPOSEDDWELLINGS) WHERE UUEID = @UUEID;

UPDATE POPULATION_EXPOSURE B SET EXPOSEDHOSPITALS = COALESCE((SELECT sum(P.EXPOSEDHOSPITALS) FROM POPULATION_EXPOSURE P,
noise_level_class C where P.NOISELEVEL = C.NOISELEVEL and C.EXPOSURENOISELEVEL=B.NOISELEVEL
and EXPOSURETYPE  = B.EXPOSURETYPE and UUEID = @UUEID), EXPOSEDHOSPITALS) WHERE UUEID = @UUEID;

UPDATE POPULATION_EXPOSURE B SET EXPOSEDSCHOOLS = COALESCE((SELECT sum(P.EXPOSEDSCHOOLS) FROM POPULATION_EXPOSURE P,
noise_level_class C where P.NOISELEVEL = C.NOISELEVEL and C.EXPOSURENOISELEVEL=B.NOISELEVEL
and EXPOSURETYPE  = B.EXPOSURETYPE and UUEID = @UUEID), EXPOSEDSCHOOLS) WHERE UUEID = @UUEID;

UPDATE POPULATION_EXPOSURE B SET CPI = COALESCE((SELECT sum(P.CPI) FROM POPULATION_EXPOSURE P,
noise_level_class C where P.NOISELEVEL = C.NOISELEVEL and C.EXPOSURENOISELEVEL=B.NOISELEVEL
and EXPOSURETYPE  = B.EXPOSURETYPE and UUEID = @UUEID), CPI) WHERE UUEID = @UUEID;

UPDATE POPULATION_EXPOSURE B SET HA = COALESCE((SELECT sum(P.HA) FROM POPULATION_EXPOSURE P,
noise_level_class C where P.NOISELEVEL = C.NOISELEVEL and C.EXPOSURENOISELEVEL=B.NOISELEVEL
and EXPOSURETYPE  = B.EXPOSURETYPE and UUEID = @UUEID), HA) WHERE UUEID = @UUEID;

UPDATE POPULATION_EXPOSURE B SET HSD = COALESCE((SELECT sum(P.HSD) FROM POPULATION_EXPOSURE P,
 noise_level_class C where P.NOISELEVEL = C.NOISELEVEL and C.EXPOSURENOISELEVEL=B.NOISELEVEL
 and EXPOSURETYPE  = B.EXPOSURETYPE and UUEID = @UUEID), HSD) WHERE UUEID = @UUEID;
