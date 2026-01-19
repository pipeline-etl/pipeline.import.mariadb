<?php

/**
 * This file contains the AbstractRangeTestCase class.
 *
 * SPDX-FileCopyrightText: Copyright 2026 Framna Netherlands B.V., Zwolle, The Netherlands
 * SPDX-License-Identifier: MIT
 */

namespace Pipeline\Import\MariaDB\Ranges\Tests;

use Lunr\Halo\LunrBaseTestCase;
use Mockery;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use Mockery\MockInterface;
use PHPUnit\Framework\MockObject\MockObject;
use Pipeline\Import\MariaDB\MariaDBTarget;
use Pipeline\Import\MariaDB\Ranges\AbstractRange;
use Psr\Log\LoggerInterface;

/**
 * This class contains common setup routines, providers
 * and shared attributes for testing the AbstractRange class.
 *
 * @covers Pipeline\Import\MariaDB\Ranges\AbstractRange
 */
abstract class AbstractRangeTestCase extends LunrBaseTestCase
{

    use MockeryPHPUnitIntegration;

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
     * @var AbstractRange&MockInterface
     */
    protected AbstractRange&MockInterface $class;

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

        $this->class = Mockery::mock(AbstractRange::class, [ $this->logger, $this->target ])
                              ->makePartial();

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
