<?php
/**
 * Piwik - Open source web analytics
 *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 * @version $Id: ArchivingProcessBenchmark.php 6955 2012-09-09 21:04:11Z capedfuzz $
 */
require_once PIWIK_INCLUDE_PATH . '/tests/PHPUnit/BenchmarkTestCase.php';

/**
 * Runs the archiving process.
 */
class ArchivingProcessBenchmark extends BenchmarkTestCase
{
	public function setUp()
	{
		BenchmarkTestCase::deleteArchiveTables();
	}
	
	/**
     * @group        Benchmarks
     * @group        ArchivingProcess
	 */
	public function testArchivingProcess()
	{
		Piwik_VisitsSummary_API::get(self::$fixture->idSite, self::$fixture->period, self::$fixture->date);
	}
}
