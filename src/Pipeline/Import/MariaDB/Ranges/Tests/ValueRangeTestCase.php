<?php

/**
 * This file contains the ValueRangeTestCase class.
 *
 * SPDX-FileCopyrightText: Copyright 2026 Framna Netherlands B.V., Zwolle, The Netherlands
 * SPDX-License-Identifier: MIT
 */

namespace Pipeline\Import\MariaDB\Ranges\Tests;

use Lunr\Halo\LunrBaseTestCase;
use PHPUnit\Framework\MockObject\MockObject;
use Pipeline\Import\MariaDB\MariaDBTarget;
use Pipeline\Import\MariaDB\Ranges\ValueRange;
use Psr\Log\LoggerInterface;

/**
 * This class contains common setup routines, providers
 * and shared attributes for testing the ValueRange class.
 *
 * @covers Pipeline\Import\MariaDB\Ranges\ValueRange
 */
abstract class ValueRangeTestCase extends LunrBaseTestCase
{

    /**
     * Mock instance of the MariaDBTarget class.
     * @var MariaDBTarget&MockObject
     */
    protected MariaDBTarget&MockObject $target;

    /**
     * Mock instance of the Logger.
     * @var LoggerInterface&MockObject
     */
    protected LoggerInterface&MockObject $logger;

    /**
     * Instance of the tested class.
     * @var ValueRange
     */
    protected ValueRange $class;

    /**
     * TestCase Constructor.
     */
    public function setUp(): void
    {
        $this->target = $this->getMockBuilder(MariaDBTarget::class)
                             ->disableOriginalConstructor()
                             ->getMock();

        $this->logger = $this->getMockBuilder(LoggerInterface::class)
                             ->getMock();

        $this->class = new ValueRange($this->logger, $this->target);

        parent::baseSetUp($this->class);
    }

    /**
     * TestCase Destructor.
     */
    public function tearDown(): void
    {
        parent::tearDown();

        unset($this->class);
        unset($this->target);
        unset($this->logger);
    }

}

?>
