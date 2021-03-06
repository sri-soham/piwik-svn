<?php
/**
 * Piwik - Open source web analytics
 *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 * @version $Id: ProxyTest.php 6499 2012-06-24 14:50:58Z SteveG $
 */
class ProxyTest extends PHPUnit_Framework_TestCase
{
    public function getAcceptableRemoteUrls()
    {
        return array(
            // piwik white list (and used in homepage)
            array('http://piwik.org/', true),

            array('http://piwik.org', true),
            array('http://qa.piwik.org/', true),
            array('http://forum.piwik.org/', true),
            array('http://dev.piwik.org/', true),
            array('http://demo.piwik.org/', true),

            // not in the piwik white list
            array('http://www.piwik.org/', false),
            array('https://piwik.org/', false),
            array('http://example.org/', false),
        );
    }

    /**
     * @dataProvider getAcceptableRemoteUrls
     * @group Plugins
     * @group Proxy
     */
    public function testIsAcceptableRemoteUrl($url, $expected)
    {
        $this->assertEquals($expected, Piwik_Proxy_Controller::isPiwikUrl($url));
    }
}

