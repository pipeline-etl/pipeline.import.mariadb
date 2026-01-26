<?php

/**
 * This file contains the StaticRange class.
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
 * Static Pipeline Data Range.
 *
 * @phpstan-import-type ContentRangeConfig from ImportInfo
 * @phpstan-import-type ProcessedItem from Node
 */
class StaticRange extends AbstractRange
{

    /**
     * Field name.
     * @var string
     */
    protected readonly string $field;

    /**
     * Field value.
     * @var int|float|string
     */
    protected readonly int|float|string $value;

    /**
     * Constructor.
     *
     * @param LoggerInterface $logger Shared instance of a logger class
     * @param MariaDBTarget   $target Shared instance of the MariaDBTarget class
     */
    public function __construct(LoggerInterface $logger, MariaDBTarget $target)
    {
        parent::__construct($logger, $target);
    }

    /**
     * Destructor.
     */
    public function __destruct()
    {
        parent::__destruct();
    }

    /**
     * Check whether the class holds an empty range.
     *
     * @return bool Always FALSE
     */
    public function isEmpty(): bool
    {
        return FALSE;
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
            throw new ContentRangeException('Static range requires a field value!');
        }

        /**
         * Safety check, since we can't ensure array field/value types
         * @phpstan-ignore identical.alwaysFalse
         */
        if (!array_key_exists('value', $config) || $config['value'] === NULL)
        {
            throw new ContentRangeException('Static range requires a column value!');
        }

        $this->field = (string) $config['field'];

        $this->value = is_bool($config['value']) ? (int) $config['value'] : $config['value'];
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
            throw new ContentRangeException('Tried to apply static range, but no query builder is active!');
        }

        $builder->where($escaper->column($this->field), $escaper->value($this->value));
    }

}

?>
