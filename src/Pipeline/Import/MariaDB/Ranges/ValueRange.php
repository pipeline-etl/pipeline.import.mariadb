<?php

/**
 * This file contains the ValueRange class.
 *
 * SPDX-FileCopyrightText: Copyright 2026 Framna Netherlands B.V., Zwolle, The Netherlands
 * SPDX-License-Identifier: MIT
 */

namespace Pipeline\Import\MariaDB\Ranges;

use Pipeline\Common\Node;
use Pipeline\Import\Exceptions\ContentRangeException;
use Pipeline\Import\ImportInfo;
use Pipeline\Import\MariaDB\MariaDBTarget;
use Psr\Log\LoggerInterface;

/**
 * Value Pipeline Data Range.
 *
 * @phpstan-import-type ContentRangeConfig from ImportInfo
 * @phpstan-import-type ProcessedItem from Node
 */
class ValueRange extends AbstractRange
{

    /**
     * Value key.
     * @var string
     */
    protected readonly string $key;

    /**
     * Values.
     * @var list<bool|float|int|string|null>
     */
    protected array $values;

    /**
     * Constructor.
     *
     * @param LoggerInterface $logger Shared instance of a logger class
     * @param MariaDBTarget   $target Shared instance of the MariaDBTarget class
     */
    public function __construct(LoggerInterface $logger, MariaDBTarget $target)
    {
        parent::__construct($logger, $target);

        $this->values = [];
    }

    /**
     * Destructor.
     */
    public function __destruct()
    {
        parent::__destruct();

        $this->values = [];
    }

    /**
     * Check whether the class holds an empty range.
     *
     * @return bool TRUE if empty, FALSE otherwise
     */
    public function isEmpty(): bool
    {
        return empty($this->values);
    }

    /**
     * Set the range data.
     *
     * @param ProcessedItem[]    $data   Full data set
     * @param ContentRangeConfig $config Range config
     *
     * @return void
     */
    public function setData(array &$data, array $config): void
    {
        /**
         * Safety check, since we can't ensure array field/value types
         * @phpstan-ignore identical.alwaysFalse
         */
        if (!array_key_exists('field', $config) || $config['field'] === NULL)
        {
            throw new ContentRangeException('Value range requires a field value!');
        }

        $this->key    = (string) $config['field'];
        $this->values = array_values(array_unique(array_column($data, $this->key)));

        if ($this->isEmpty())
        {
            throw new ContentRangeException("Value range can't be empty!");
        }

        $this->values = array_values(array_filter($this->values, fn($v) => $v !== NULL));

        if ($this->isEmpty())
        {
            throw new ContentRangeException('Only NULL values in value range!');
        }

        foreach ($this->values as &$value)
        {
            $value = is_bool($value) ? (int) $value : $value;
        }

        unset($value);
    }

    /**
     * Apply range.
     *
     * @return void
     */
    public function apply(): void
    {
        $builder = $this->target->getQueryBuilder();
        $escaper = $this->target->getQueryEscaper();

        if ($builder === NULL)
        {
            throw new ContentRangeException('Tried to apply value range, but no query builder is active!');
        }

        $data = array_map([ $escaper, 'value' ], $this->values);

        $builder->where_in($escaper->column($this->key), $escaper->list_value($data));
    }

}

?>
