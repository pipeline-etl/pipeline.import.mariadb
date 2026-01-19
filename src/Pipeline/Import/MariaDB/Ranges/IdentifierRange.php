<?php

/**
 * This file contains the IdentifierRange class.
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
 * Identifier Pipeline Data Range.
 *
 * @phpstan-import-type ContentRangeConfig from ImportInfo
 * @phpstan-import-type ProcessedItem from Node
 */
class IdentifierRange extends AbstractRange
{

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
     * Set the range data.
     *
     * @param ProcessedItem[]    $data   Full data set
     * @param ContentRangeConfig $config Range config
     *
     * @return void
     */
    public function setData(array &$data, array $config): void
    {
        $keys = $this->target->getIdentifierKeys();

        foreach ($data as $item)
        {
            $this->data[] = array_filter($item, function ($k) use ($keys) {
                return in_array($k, $keys, TRUE);
            }, ARRAY_FILTER_USE_KEY);
        }

        if ($this->isEmpty())
        {
            throw new ContentRangeException("Identifier range can't be empty!");
        }
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
            throw new ContentRangeException('Tried to apply identifier range, but no query builder is active!');
        }

        while ($item = array_shift($this->data))
        {
            $builder->start_where_group();

            foreach ($item as $key => $value)
            {
                if (is_bool($value))
                {
                    $builder->where($escaper->column($key), (string) $escaper->intvalue($value));
                }
                else
                {
                    $builder->where($escaper->column($key), $escaper->value($value));
                }
            }

            $builder->end_where_group();
            $builder->or();
        }
    }

}

?>
