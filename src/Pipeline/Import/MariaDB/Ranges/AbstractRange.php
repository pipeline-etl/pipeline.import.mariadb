<?php

/**
 * This file contains the AbstractRange class.
 *
 * SPDX-FileCopyrightText: Copyright 2026 Framna Netherlands B.V., Zwolle, The Netherlands
 * SPDX-License-Identifier: MIT
 */

namespace Pipeline\Import\MariaDB\Ranges;

use Pipeline\Common\Node;
use Pipeline\Import\ContentRangeInterface;
use Pipeline\Import\ImportInfo;
use Pipeline\Import\MariaDB\MariaDBTarget;
use Psr\Log\LoggerInterface;

/**
 * Abstract Pipeline Data Range.
 *
 * @phpstan-import-type ContentRangeConfig from ImportInfo
 * @phpstan-import-type ProcessedItem from Node
 */
abstract class AbstractRange extends Node implements ContentRangeInterface
{

    /**
     * Shared instance of the MariaDBTarget class
     * @var MariaDBTarget
     */
    protected readonly MariaDBTarget $target;

    /**
     * Range data
     * @var ProcessedItem[]
     */
    protected array $data;

    /**
     * Constructor.
     *
     * @param LoggerInterface $logger Shared instance of a logger class
     * @param MariaDBTarget   $target Shared instance of the MariaDBTarget class
     */
    public function __construct(LoggerInterface $logger, MariaDBTarget $target)
    {
        parent::__construct($logger);

        $this->target = $target;
        $this->data   = [];
    }

    /**
     * Destructor.
     */
    public function __destruct()
    {
        parent::__destruct();

        $this->data = [];
    }

    /**
     * Check whether the class holds an empty range.
     *
     * @return bool TRUE if empty, FALSE otherwise
     */
    public function isEmpty(): bool
    {
        return empty($this->data);
    }

}

?>
