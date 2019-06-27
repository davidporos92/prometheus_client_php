<?php

namespace Prometheus\Storage;

class Mongo implements \Prometheus\Storage\Adapter
{
    private static $defaultOptions = array();
    
    private $options;
    private $mongo;
    
    const COLLECTION_COUNTERS = 'counters';
    const COLLECTION_GAUGES = 'gauges';
    const COLLECTION_HISTOGRAMS = 'histograms';
    
    public function __construct(array $options = array())
    {
        // with php 5.3 we cannot initialize the options directly on the field definition
        // so we initialize them here for now
        if (!isset(self::$defaultOptions['server'])) {
            self::$defaultOptions['server'] = 'mongodb://localhost:27017';
        }
        if (!isset(self::$defaultOptions['database'])) {
            self::$defaultOptions['database'] = 'prometheus_metrics';
        }
        if (!isset(self::$defaultOptions['driverOptions'])) {
            self::$defaultOptions['driverOptions']['typeMap'] = [
                'array' => 'array',
                'document' => 'array',
                'root' => 'array',
            ];
        }
        
        $this->options = array_merge(self::$defaultOptions, $options);
        $this->mongo = new \MongoDB\Client(
            $this->options['server'],
            [],
            $this->options['driverOptions']
        );
    }
    
    /**
     * @param array $options
     */
    public static function setDefaultOptions(array $options)
    {
        self::$defaultOptions = array_merge(self::$defaultOptions, $options);
    }
    
    public function flushMongo()
    {
        $this->getDatabase()->drop();
    }
    
    /**
     * @return \MongoDB\Database
     */
    public function getDatabase()
    {
        return $this->mongo->{$this->options['database']};
    }
    
    /**
     * @return \MongoDB\Collection
     */
    private function getCountersCollection()
    {
        return $this
            ->getDatabase()
            ->selectCollection(self::COLLECTION_COUNTERS);
    }
    
    /**
     * @return \MongoDB\Collection
     */
    private function getGaugesCollection()
    {
        return $this
            ->getDatabase()
            ->selectCollection(self::COLLECTION_GAUGES);
    }
    
    /**
     * @return \MongoDB\Collection
     */
    private function getHistogramsCollection()
    {
        return $this
            ->getDatabase()
            ->selectCollection(self::COLLECTION_HISTOGRAMS);
    }
    
    /**
     * @return \MongoDB\Driver\Cursor
     */
    private function getCounters()
    {
        return $this->getCountersCollection()->find();
    }
    
    /**
     * @return \MongoDB\Driver\Cursor
     */
    private function getGauges()
    {
        return $this->getGaugesCollection()->find();
    }
    
    /**
     * @return \MongoDB\Driver\Cursor
     */
    private function getHistograms()
    {
        return $this->getHistogramsCollection()->find();
    }
    
    /**
     * @param $metaKey
     * @return array|null|object
     */
    private function findHistogram($metaKey)
    {
        return (array)$this
            ->getHistogramsCollection()
            ->findOne(['meta.key' => $metaKey]);
    }
    
    /**
     * @param $metaKey
     * @param array $histogramData
     * @return \MongoDB\UpdateResult
     */
    private function saveHistogram($metaKey, array $histogramData)
    {
        return $this
            ->getHistogramsCollection()
            ->updateOne(
                ['meta.key' => $metaKey],
                ['$set' => $histogramData],
                ['upsert' => true]
            );
    }
    
    /**
     * @param array $data
     */
    public function updateHistogram(array $data)
    {
        // Initialize the sum
        $metaKey = $this->metaKey($data);
        $histogram = $this->findHistogram($metaKey);
        
        if (empty($histogram)) {
            $histogram = [
                'meta' => $this->metaData($data, $metaKey),
                'samples' => []
            ];
        }
        
        $sumKey = $this->histogramBucketValueKey($data, 'sum');
        if (array_key_exists($sumKey, $histogram['samples']) === false) {
            $histogram['samples'][$sumKey] = 0;
        }
        
        $histogram['samples'][$sumKey] += $data['value'];
        
        $bucketToIncrease = '+Inf';
        foreach ($data['buckets'] as $bucket) {
            if ($data['value'] <= $bucket) {
                $bucketToIncrease = $bucket;
                break;
            }
        }
        
        $bucketKey = $this->histogramBucketValueKey($data, $bucketToIncrease);
        if (array_key_exists($bucketKey, $histogram['samples']) === false) {
            $histogram['samples'][$bucketKey] = 0;
        }
        $histogram['samples'][$bucketKey] += 1;
        
        $this->saveHistogram($metaKey, $histogram);
    }
    
    /**
     * @param array $data
     * @param $bucket
     * @return string
     */
    private function histogramBucketValueKey(array $data, $bucket)
    {
        return implode(
            ':',
            [
                $data['type'],
                $data['name'],
                json_encode($data['labelValues']),
                $bucket
            ]
        );
    }
    
    /**
     * @param $metaKey
     * @return array|null|object
     */
    private function findGauge($metaKey)
    {
        return (array)$this
            ->getGaugesCollection()
            ->findOne(['meta.key' => $metaKey]);
    }
    
    /**
     * @param $metaKey
     * @param array $gaugeData
     * @return \MongoDB\UpdateResult
     */
    private function saveGauge($metaKey, array $gaugeData)
    {
        return $this
            ->getGaugesCollection()
            ->updateOne(
                ['meta.key' => $metaKey],
                ['$set' => $gaugeData],
                ['upsert' => true]
            );
    }
    
    /**
     * @param array $data
     */
    public function updateGauge(array $data)
    {
        $metaKey = $this->metaKey($data);
        $valueKey = $this->valueKey($data);
        
        $gauge = $this->findGauge($metaKey);
        if (empty($gauge)) {
            $gauge = [
                'meta' => $this->metaData($data, $metaKey),
                'samples' => []
            ];
        }
        if (!isset($gauge['samples'][$valueKey])) {
            $gauge['samples'][$valueKey] = 0;
        }
        if ($data['command'] === \Prometheus\Storage\Adapter::COMMAND_SET) {
            $gauge['samples'][$valueKey] = $data['value'];
        } else {
            $gauge['samples'][$valueKey] += $data['value'];
        }
        
        $this->saveGauge($metaKey, $gauge);
    }
    
    /**
     * @param $metaKey
     * @return array|null|object
     */
    private function findCounter($metaKey)
    {
        return (array)$this
            ->getCountersCollection()
            ->findOne(['meta.key' => $metaKey]);
    }
    
    /**
     * @param $metaKey
     * @param array $counterData
     * @return \MongoDB\UpdateResult
     */
    private function saveCounter($metaKey, array $counterData)
    {
        return $this
            ->getCountersCollection()
            ->updateOne(
                ['meta.key' => $metaKey],
                ['$set' => $counterData],
                ['upsert' => true]
            );
    }
    
    /**
     * @param array $data
     */
    public function updateCounter(array $data)
    {
        $metaKey = $this->metaKey($data);
        $valueKey = $this->valueKey($data);
        
        $counter = $this->findCounter($metaKey);
        if (empty($counter)) {
            $counter = [
                'meta' => $this->metaData($data, $metaKey),
                'samples' => []
            ];
        }
        if (!isset($counter['samples'][$valueKey])) {
            $counter['samples'][$valueKey] = 0;
        }
        if ($data['command'] === \Prometheus\Storage\Adapter::COMMAND_SET) {
            $counter['samples'][$valueKey] = 0;
        } else {
            $counter['samples'][$valueKey] += $data['value'];
        }
        
        $this->saveCounter($metaKey, $counter);
    }
    
    /**
     * @return \Prometheus\MetricFamilySamples[]
     */
    public function collect()
    {
        return array_merge(
            $this->internalCollect($this->getCounters()),
            $this->internalCollect($this->getGauges()),
            $this->collectHistograms()
        );
    }
    
    /**
     * @param \MongoDB\Driver\Cursor $metrics
     * @return array
     */
    private function internalCollect(\MongoDB\Driver\Cursor $metrics)
    {
        $result = [];
        foreach ($metrics as $metric) {
            $metaData = $metric['meta'];
            $data = [
                'name' => $metaData['name'],
                'help' => $metaData['help'],
                'type' => $metaData['type'],
                'labelNames' => $metaData['labelNames'],
            ];
            foreach ($metric['samples'] as $key => $value) {
                $parts = explode(':', $key);
                $labelValues = $parts[2];
                $data['samples'][] = [
                    'name' => $metaData['name'],
                    'labelNames' => [],
                    'labelValues' => json_decode($labelValues),
                    'value' => $value
                ];
            }
            $this->sortSamples($data['samples']);
            $result[] = new \Prometheus\MetricFamilySamples($data);
        }
        return $result;
    }
    
    /**
     * @return array
     */
    private function collectHistograms()
    {
        $histograms = [];
        foreach ($this->getHistograms() as $histogram) {
            $metaData = $histogram['meta'];
            $data = [
                'name' => $metaData['name'],
                'help' => $metaData['help'],
                'type' => $metaData['type'],
                'labelNames' => $metaData['labelNames'],
                'buckets' => $metaData['buckets']
            ];
            
            // Add the Inf bucket so we can compute it later on
            $data['buckets'][] = '+Inf';
            
            $histogramBuckets = [];
            foreach ($histogram['samples'] as $key => $value) {
                $parts = explode(':', $key);
                $labelValues = $parts[2];
                $bucket = $parts[3];
                // Key by labelValues
                $histogramBuckets[$labelValues][$bucket] = $value;
            }
            
            // Compute all buckets
            $labels = array_keys($histogramBuckets);
            sort($labels);
            foreach ($labels as $labelValues) {
                $acc = 0;
                $decodedLabelValues = json_decode($labelValues);
                foreach ($data['buckets'] as $bucket) {
                    $bucket = (string)$bucket;
                    if (!isset($histogramBuckets[$labelValues][$bucket])) {
                        $data['samples'][] = [
                            'name' => $metaData['name'] . '_bucket',
                            'labelNames' => ['le'],
                            'labelValues' => array_merge(
                                $decodedLabelValues, [$bucket]
                            ),
                            'value' => $acc
                        ];
                    } else {
                        $acc += $histogramBuckets[$labelValues][$bucket];
                        $data['samples'][] = [
                            'name' => $metaData['name'] . '_' . 'bucket',
                            'labelNames' => ['le'],
                            'labelValues' => array_merge(
                                $decodedLabelValues, [$bucket]
                            ),
                            'value' => $acc
                        ];
                    }
                }
                
                // Add the count
                $data['samples'][] = [
                    'name' => $metaData['name'] . '_count',
                    'labelNames' => [],
                    'labelValues' => $decodedLabelValues,
                    'value' => $acc
                ];
                
                // Add the sum
                $data['samples'][] = [
                    'name' => $metaData['name'] . '_sum',
                    'labelNames' => [],
                    'labelValues' => $decodedLabelValues,
                    'value' => $histogramBuckets[$labelValues]['sum']
                ];
                
            }
            $histograms[] = new \Prometheus\MetricFamilySamples($data);
        }
        return $histograms;
    }
    
    /**
     * @param array $data
     *
     * @return string
     */
    private function metaKey(array $data)
    {
        return implode(':', [$data['type'], $data['name'], 'meta']);
    }
    
    /**
     * @param array $data
     *
     * @return string
     */
    private function valueKey(array $data)
    {
        return implode(
            ':',
            [
                $data['type'],
                $data['name'],
                json_encode($data['labelValues']),
                'value'
            ]
        );
    }
    
    /**
     * @param array $data
     *
     * @return array
     */
    private function metaData(array $data, $metaKey)
    {
        $metricsMetaData = $data;
        unset($metricsMetaData['value']);
        unset($metricsMetaData['command']);
        unset($metricsMetaData['labelValues']);
        $metricsMetaData['key'] = $metaKey;
        return $metricsMetaData;
    }
    
    /**
     * @param array $samples
     */
    private function sortSamples(array &$samples)
    {
        usort(
            $samples,
            function ($a, $b) {
                return strcmp(
                    implode("", $a['labelValues']),
                    implode("", $b['labelValues'])
                );
            }
        );
    }
}
