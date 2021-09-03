WITH AnomalyDetectionStep AS
(
    SELECT
        DeviceId,
        SensorReadings.temperature AS temperature,
        AnomalyDetection_SpikeAndDip(CAST(SensorReadings.temperature AS float), 95, 60, 'spikesanddips')
            OVER(LIMIT DURATION(second, 60)) AS SpikeAndDipScores
    FROM sanainput
)
SELECT
    DeviceId,
    temperature,
    CAST(GetRecordPropertyValue(SpikeAndDipScores, 'Score') AS float) AS
    SpikeAndDipScore,
    CAST(GetRecordPropertyValue(SpikeAndDipScores, 'IsAnomaly') AS bigint) AS
    IsSpikeAndDipAnomaly
INTO sanaoutput
FROM AnomalyDetectionStep
WHERE CAST(GetRecordPropertyValue(SpikeAndDipScores, 'IsAnomaly') AS bigint) = 1