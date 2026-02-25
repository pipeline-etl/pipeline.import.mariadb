<?php

/**
 * This file contains the MariaDBTargetDeleteDataTest class.
 *
 * SPDX-FileCopyrightText: Copyright 2025 Framna Netherlands B.V., Zwolle, The Netherlands
 * SPDX-License-Identifier: MIT
 */

namespace Pipeline\Import\MariaDB\Tests;

use Lunr\Gravity\Exceptions\DeadlockException;
use Lunr\Gravity\Exceptions\QueryException;
use Lunr\Gravity\Tests\Helpers\DatabaseAccessObjectQueryTestTrait;
use Pipeline\Import\ContentRangeInterface;
use Pipeline\Import\MariaDB\Ranges\IdentifierRange;

/**
 * This class contains the tests for the deleteData function of the MariaDBTarget class.
 *
 * @covers Pipeline\Import\MariaDB\MariaDBTarget
 */
class MariaDBTargetDeleteDataTest extends MariaDBTargetTestCase
{

    use DatabaseAccessObjectQueryTestTrait;

    private const LOCALIZED_ENTITIES = [
        [ 'id' => 'a1', 'language' => 'en-US', 'revision' => 3, 'name' => 'yeah' ],
        [ 'id' => 'b2', 'language' => 'fr-FR', 'revision' => 5, 'name' => 'he' ],
    ];

    private const NON_LOCALIZED_ENTITIES = [
        [ 'id' => 'a1', 'revision' => 3, 'url' => 'http://ida1.org' ],
        [ 'id' => 'b2', 'revision' => 5, 'url' => 'http://idb2.org' ],
    ];

    /**
     * Unit Test Data Provider for localized entities data.
     *
     * @return array $entities localized entities data.
     */
    public static function localizedEntitiesDataProvider(): array
    {
        $entities = [];

        $entities[] = [ [ self::LOCALIZED_ENTITIES[0] ] ];
        $entities[] = [ self::LOCALIZED_ENTITIES ];

        return $entities;
    }

    /**
     * Unit Test Data Provider for non localized entities data.
     *
     * @return array $entities non localized entities data.
     */
    public static function nonLocalizedEntitiesDataProvider(): array
    {
        $entities = [];

        $entities[] = [ [ self::NON_LOCALIZED_ENTITIES[0] ] ];
        $entities[] = [ self::NON_LOCALIZED_ENTITIES ];

        return $entities;
    }

    /**
     * Test that deleteData() returns 0 if there is no entities to delete.
     *
     * @covers Pipeline\Import\MariaDB\MariaDBTarget::deleteData
     */
    public function testDeleteDataWithNoDataReturnsZero(): void
    {
        $this->setReflectionPropertyValue('identifierKeys', [ 'id' ]);
        $method = $this->getReflectionMethod('deleteData');

        $param  = [];
        $result = $method->invokeArgs($this->class, [ &$param ]);

        $this->assertEquals(0, $result);
    }

    /**
     * Test that deleteData() constructs a correct SQL query for a non localized entity.
     *
     * @param array $entities Non-localized entities
     *
     * @dataProvider nonLocalizedEntitiesDataProvider
     * @covers       Pipeline\Import\MariaDB\MariaDBTarget::deleteData
     */
    public function testDeleteDataConstructsCorrectRealQueryForNonLocalizedEntity(array $entities): void
    {
        $this->setReflectionPropertyValue('identifierKeys', [ 'id' ]);
        $this->setReflectionPropertyValue('table', 'table');

        $this->expectQuerySuccess();

        $this->result->shouldReceive('affected_rows')
                     ->once()
                     ->andReturn(0);

        if (count($entities) === 1)
        {
            $expectedFile = TEST_STATICS . '/sql/MariaDBTarget/deleteData_nonlocalized.sql';
        }
        else
        {
            $expectedFile = TEST_STATICS . '/sql/MariaDBTarget/deleteData_nonlocalized_multiple.sql';
        }

        $method = $this->getReflectionMethod('deleteData');

        $method->invokeArgs($this->class, [ &$entities ]);

        $sql = $this->realBuilder->get_delete_query();

        $this->assertSqlStringEqualsSqlFile($expectedFile, $sql);
    }

    /**
     * Test that deleteData() constructs a correct SQL query for a non localized entity.
     *
     * @covers Pipeline\Import\MariaDB\MariaDBTarget::deleteData
     */
    public function testDeleteDataConstructsCorrectRealQueryForBooleanIdentifier(): void
    {
        $entities = [
            [ 'id' => TRUE, 'revision' => 3, 'url' => 'http://ida1.org' ],
            [ 'id' => FALSE, 'revision' => 5, 'url' => 'http://idb2.org' ],
        ];

        $this->setReflectionPropertyValue('identifierKeys', [ 'id' ]);
        $this->setReflectionPropertyValue('table', 'table');

        $this->expectQuerySuccess();

        $this->result->shouldReceive('affected_rows')
                     ->once()
                     ->andReturn(0);

        $method = $this->getReflectionMethod('deleteData');

        $method->invokeArgs($this->class, [ &$entities ]);

        $sql = $this->realBuilder->get_delete_query();

        $this->assertSqlStringEqualsSqlFile(TEST_STATICS . '/sql/MariaDBTarget/deleteData_boolean_identifier.sql', $sql);
    }

    /**
     * Test that deleteData() constructs a correct SQL query.
     *
     * @param array $entities Non-localized entities
     *
     * @dataProvider nonLocalizedEntitiesDataProvider
     * @covers       Pipeline\Import\MariaDB\MariaDBTarget::deleteData
     */
    public function testDeleteDataConstructsCorrectRealQueryWithRange(array $entities): void
    {
        $this->setReflectionPropertyValue('identifierKeys', [ 'id' ]);
        $this->setReflectionPropertyValue('table', 'table');

        $range1 = $this->getMockBuilder(ContentRangeInterface::class)
                       ->getMock();

        $range2 = $this->getMockBuilder(ContentRangeInterface::class)
                       ->getMock();

        $ranges = [ $range1, $range2 ];

        $this->expectQuerySuccess();

        $this->result->shouldReceive('affected_rows')
                     ->once()
                     ->andReturn(0);

        $range1->expects($this->once())
               ->method('apply')
               ->willReturnCallback(function () {
                   $builder = $this->class->getQueryBuilder();
                   $escaper = $this->class->getQueryEscaper();

                   $builder->where($escaper->column('range1'), $escaper->value('value1'));
               });

        $range2->expects($this->once())
               ->method('apply')
               ->willReturnCallback(function () {
                   $builder = $this->class->getQueryBuilder();
                   $escaper = $this->class->getQueryEscaper();

                   $builder->where($escaper->column('range2'), $escaper->value('value2'));
               });

        if (count($entities) === 1)
        {
            $expectedFile = TEST_STATICS . '/sql/MariaDBTarget/deleteData_with_range.sql';
        }
        else
        {
            $expectedFile = TEST_STATICS . '/sql/MariaDBTarget/deleteData_with_range_multiple.sql';
        }

        $method = $this->getReflectionMethod('deleteData');

        $method->invokeArgs($this->class, [ &$entities, $ranges ]);

        $sql = $this->realBuilder->get_delete_query();

        $this->assertSqlStringEqualsSqlFile($expectedFile, $sql);
    }

    /**
     * Test that deleteData() constructs a correct SQL query.
     *
     * @param array $entities Non-localized entities
     *
     * @dataProvider nonLocalizedEntitiesDataProvider
     * @covers       Pipeline\Import\MariaDB\MariaDBTarget::deleteData
     */
    public function testDeleteDataConstructsCorrectRealQueryWithIdentifierRange(array $entities): void
    {
        $this->setReflectionPropertyValue('identifierKeys', [ 'id' ]);
        $this->setReflectionPropertyValue('table', 'table');

        $range1 = $this->getMockBuilder(IdentifierRange::class)
                       ->disableOriginalConstructor()
                       ->getMock();

        $range2 = $this->getMockBuilder(ContentRangeInterface::class)
                       ->getMock();

        $ranges = [ $range1, $range2 ];

        $this->expectQuerySuccess();

        $this->result->shouldReceive('affected_rows')
                     ->once()
                     ->andReturn(0);

        $range1->expects($this->once())
               ->method('isEmpty')
               ->willReturn(FALSE);

        $range2->expects($this->once())
               ->method('isEmpty')
               ->willReturn(FALSE);

        $range1->expects($this->never())
               ->method('apply');

        $range2->expects($this->once())
               ->method('apply')
               ->willReturnCallback(function () {
                   $builder = $this->class->getQueryBuilder();
                   $escaper = $this->class->getQueryEscaper();

                   $builder->where($escaper->column('range2'), $escaper->value('value2'));
               });

        if (count($entities) === 1)
        {
            $expectedFile = TEST_STATICS . '/sql/MariaDBTarget/deleteData_with_identifier_range.sql';
        }
        else
        {
            $expectedFile = TEST_STATICS . '/sql/MariaDBTarget/deleteData_with_identifier_range_multiple.sql';
        }

        $method = $this->getReflectionMethod('deleteData');

        $method->invokeArgs($this->class, [ &$entities, $ranges ]);

        $sql = $this->realBuilder->get_delete_query();

        $this->assertSqlStringEqualsSqlFile($expectedFile, $sql);
    }

    /**
     * Test that deleteData() constructs a correct SQL query.
     *
     * @param array $entities Non-localized entities
     *
     * @dataProvider nonLocalizedEntitiesDataProvider
     * @covers       Pipeline\Import\MariaDB\MariaDBTarget::deleteData
     */
    public function testDeleteDataConstructsCorrectRealQueryWithEmptyRange(array $entities): void
    {
        $this->setReflectionPropertyValue('identifierKeys', [ 'id' ]);
        $this->setReflectionPropertyValue('table', 'table');

        $range1 = $this->getMockBuilder(ContentRangeInterface::class)
                       ->getMock();

        $range2 = $this->getMockBuilder(ContentRangeInterface::class)
                       ->getMock();

        $ranges = [ $range1, $range2 ];

        $this->expectQuerySuccess();

        $this->result->shouldReceive('affected_rows')
                     ->once()
                     ->andReturn(0);

        $range1->expects($this->once())
               ->method('isEmpty')
               ->willReturn(TRUE);

        $range2->expects($this->once())
               ->method('isEmpty')
               ->willReturn(TRUE);

        if (count($entities) === 1)
        {
            $expectedFile = TEST_STATICS . '/sql/MariaDBTarget/deleteData_nonlocalized.sql';
        }
        else
        {
            $expectedFile = TEST_STATICS . '/sql/MariaDBTarget/deleteData_nonlocalized_multiple.sql';
        }

        $method = $this->getReflectionMethod('deleteData');

        $method->invokeArgs($this->class, [ &$entities, $ranges ]);

        $sql = $this->realBuilder->get_delete_query();

        $this->assertSqlStringEqualsSqlFile($expectedFile, $sql);
    }

    /**
     * Test that deleteData() throws an exception in case of a query error.
     *
     * @covers Pipeline\Import\MariaDB\MariaDBTarget::deleteData
     */
    public function testDeleteDataThrowsExceptionIfQueryFailed(): void
    {
        $this->expectQueryError();

        $this->setReflectionPropertyValue('identifierKeys', [ 'id' ]);
        $this->setReflectionPropertyValue('table', 'table');

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Database query error!');

        $context = [ 'query' => 'QUERY;', 'error' => 'Error!' ];

        $this->logger->expects($this->once())
                     ->method('error')
                     ->with('{query}; failed with error: {error}', $context);

        $method = $this->getReflectionMethod('deleteData');

        $localizedEntities = self::LOCALIZED_ENTITIES;

        $method->invokeArgs($this->class, [ &$localizedEntities ]);
    }

    /**
     * Test that deleteData() throws an exception in case of a query deadlock.
     *
     * @covers Pipeline\Import\MariaDB\MariaDBTarget::deleteData
     */
    public function testDeleteDataThrowsExceptionOnDeadlock(): void
    {
        $this->setReflectionPropertyValue('identifierKeys', [ 'id' ]);
        $this->setReflectionPropertyValue('table', 'table');

        $this->db->shouldReceive('get_new_dml_query_builder_object')
                 ->once()
                 ->with(FALSE)
                 ->andReturn($this->realBuilder);

        $this->db->shouldReceive('query')
                 ->zeroOrMoreTimes()
                 ->andReturn($this->result);

        $this->result->shouldReceive('warnings')
                     ->once()
                     ->andReturn(NULL);

        $this->result->shouldReceive('has_failed')
                     ->once()
                     ->andReturn(TRUE);

        $this->result->shouldReceive('error_number')
                     ->once()
                     ->andReturn(1);

        $this->result->shouldReceive('error_message')
                     ->twice()
                     ->andReturn('Error!');

        $this->result->shouldReceive('query')
                     ->times(7)
                     ->andReturn('QUERY');

        $this->result->shouldReceive('has_deadlock')
                     ->times(6)
                     ->andReturn(TRUE);

        $this->result->shouldReceive('has_lock_timeout')
                     ->zeroOrMoreTimes()
                     ->andReturn(FALSE);

        $context = [ 'query' => 'QUERY', 'error' => 'Error!' ];

        $this->logger->expects($this->once())
                     ->method('error')
                     ->with('{query}; failed with error: {error}', $context);

        $this->expectException(DeadlockException::class);
        $this->expectExceptionMessage('Database query deadlock!');

        $method = $this->getReflectionMethod('deleteData');

        $localizedEntities = self::LOCALIZED_ENTITIES;

        $method->invokeArgs($this->class, [ &$localizedEntities ]);
    }

    /**
     * Test that deleteData() returns the number of affected rows on success.
     *
     * @covers Pipeline\Import\MariaDB\MariaDBTarget::deleteData
     */
    public function testDeleteDataReturnsNumberOfAffectedRowsOnSuccessfulQuery(): void
    {
        $this->expectQuerySuccess();

        $this->setReflectionPropertyValue('identifierKeys', [ 'id' ]);
        $this->setReflectionPropertyValue('table', 'table');

        $this->result->shouldReceive('affected_rows')
                     ->once()
                     ->andReturn(10);

        $method = $this->getReflectionMethod('deleteData');

        $localizedEntities = self::LOCALIZED_ENTITIES;

        $result = $method->invokeArgs($this->class, [ &$localizedEntities ]);

        $this->assertEquals(10, $result);
    }

    /**
     * Test that deleteData() returns the number of affected rows on success.
     *
     * @covers Pipeline\Import\MariaDB\MariaDBTarget::deleteData
     */
    public function testDeleteDataReturnsNumberOfAffectedRowsAfterDeadlockRetry(): void
    {
        $this->expectQuerySuccessAfterRetry();

        $this->setReflectionPropertyValue('identifierKeys', [ 'id' ]);
        $this->setReflectionPropertyValue('table', 'table');

        $this->result->shouldReceive('affected_rows')
                     ->once()
                     ->andReturn(10);

        $method = $this->getReflectionMethod('deleteData');

        $localizedEntities = self::LOCALIZED_ENTITIES;

        $result = $method->invokeArgs($this->class, [ &$localizedEntities ]);

        $this->assertEquals(10, $result);
    }

}

?>
