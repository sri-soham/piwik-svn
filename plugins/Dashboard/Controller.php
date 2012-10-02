<?php
/**
 * Piwik - Open source web analytics
 * 
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 * @version $Id: Controller.php 6156 2012-04-03 21:15:52Z SteveG $
 * 
 * @category Piwik_Plugins
 * @package Piwik_Dashboard
 */

/**
 *
 * @package Piwik_Dashboard
 */
class Piwik_Dashboard_Controller extends Piwik_Controller
{
	protected function getDashboardView($template)
	{
		$view = Piwik_View::factory($template);
		$this->setGeneralVariablesView($view);

		$view->availableWidgets = Piwik_Common::json_encode(Piwik_GetWidgetsList());
		$view->availableLayouts = $this->getAvailableLayouts();
		
		$view->dashboardId      = Piwik_Common::getRequestVar('idDashboard', 1, 'int');
		$view->dashboardLayout  = $this->getLayout($view->dashboardId);
		return $view;
	}
	
	public function embeddedIndex()
	{
		$view = $this->getDashboardView('index');

		echo $view->render();
	}
	
	public function index()
	{
		$view = $this->getDashboardView('standalone');
		$view->dashboards = array();
		if (!Piwik::isUserIsAnonymous()) {
			$login = Piwik::getCurrentUserLogin();

			$view->dashboards = Piwik_Dashboard::getAllDashboards($login);
		}
		echo $view->render();
	}
	
	public function getAvailableWidgets()
	{
		$this->checkTokenInUrl();
	    echo Piwik_Common::json_encode(Piwik_GetWidgetsList());
	}
	
	public function getDashboardLayout()
	{
		$this->checkTokenInUrl();
		
		$idDashboard = Piwik_Common::getRequestVar('idDashboard', 1, 'int');

		$layout = $this->getLayout($idDashboard);
		
		echo $layout;
	}
	
	public function resetLayout() 
	{
		$this->checkTokenInUrl();
		$layout = $this->getDefaultLayout();
		$idDashboard = Piwik_Common::getRequestVar('idDashboard', 1, 'int' );
		if(Piwik::isUserIsAnonymous())
		{
			$session = new Piwik_Session_Namespace("Piwik_Dashboard");
			$session->dashboardLayout = $layout;
			$session->setExpirationSeconds(1800);
		}
		else
		{
		    $this->saveLayoutForUser(Piwik::getCurrentUserLogin(),$idDashboard, $layout);
		}
	}
	
	/**
	 * Records the layout in the DB for the given user.
	 *
	 * @param string $login
	 * @param int $idDashboard
	 * @param string $layout
	 */
	protected function saveLayoutForUser( $login, $idDashboard, $layout)
	{
		$UserDashboard = Piwik_Db_Factory::getDAO('user_dashboard');
		$UserDashboard->saveLayout($login, $idDashboard, $layout);
	}

	/**
	 * Updates the name of a dashboard
	 *
	 * @param string $login
	 * @param int $idDashboard
	 * @param string $name
	 */
	protected function updateDashboardName( $login, $idDashboard, $name ) {
		$UserDashboard = Piwik_Db_Factory::getDAO('user_dashboard');
		$UserDashboard->updateName($name, $login, $idDashboard);
	}
	
	/**
	 * Returns the layout in the DB for the given user, or false if the layout has not been set yet.
	 * Parameters must be checked BEFORE this function call
	 *
	 * @param string $login
	 * @param int $idDashboard
     * @return bool
     */
	protected function getLayoutForUser( $login, $idDashboard)
	{
		$UserDashboard = Piwik_Db_Factory::getDAO('user_dashboard');
		$return = $UserDashboard->getLayoutByLoginDashboard($login, $idDashboard);
		if(count($return) == 0)
		{
			return false;
		}
		return $return[0]['layout'];
	}

	/**
	 * Removes the dashboard with the given id
     */
	public function removeDashboard()
	{
		$this->checkTokenInUrl();

		if (Piwik::isUserIsAnonymous()) {
			return;
		}

		$idDashboard = Piwik_Common::getRequestVar('idDashboard', 1, 'int');

		// first layout can't be removed
		if($idDashboard != 1) {
			$UserDashboard = Piwik_Db_Factory::getDAO('user_dashboard');
			$UserDashboard->deleteByLoginDashboard(
				Piwik::getCurrentUserLogin(),
				$idDashboard
			);
		}
	}

	/**
	 * Outputs all available dashboards for the current user as a JSON string
	 */
	function getAllDashboards()
	{
		$this->checkTokenInUrl();

		if (!Piwik::isUserIsAnonymous()) {
			$login = Piwik::getCurrentUserLogin();

			$dashboards = Piwik_Dashboard::getAllDashboards($login);

			echo Piwik_Common::json_encode($dashboards);
		} else {
			echo '[]';
		}
	}

	public function createNewDashboard()
	{
		$this->checkTokenInUrl();

		if (!Piwik::isUserIsAnonymous()) {
			$login = Piwik::getCurrentUserLogin();

			$UserDashboard = Piwik_Db_Factory::getDAO('user_dashboard');
			$nextId = $UserDashboard->getNextIdByLogin($login);

			$name = urldecode(Piwik_Common::getRequestVar('name', '', 'string'));
			$type = urldecode(Piwik_Common::getRequestVar('type', 'default', 'string'));
			$layout = '{}';
			if($type == 'default') {
				$layout = $this->getDefaultLayout();
			}

			$UserDashboard->newDashboard($login, $nextId, $name, $layout);
			echo Piwik_Common::json_encode($nextId);
		} else {
			echo '0';
		}
	}

	/**
	 * Saves the layout for the current user
	 * anonymous = in the session
	 * authenticated user = in the DB
	 */
	public function saveLayout()
	{
		$this->checkTokenInUrl();

		$layout      = Piwik_Common::unsanitizeInputValue(Piwik_Common::getRequestVar('layout'));
		$idDashboard = Piwik_Common::getRequestVar('idDashboard', 1, 'int' );
		$name        = Piwik_Common::getRequestVar('name', '', 'string' );
		if(Piwik::isUserIsAnonymous())
		{
			$session = new Piwik_Session_Namespace("Piwik_Dashboard");
			$session->dashboardLayout = $layout;
			$session->setExpirationSeconds(1800);
		}
		else
		{
			$this->saveLayoutForUser(Piwik::getCurrentUserLogin(),$idDashboard, $layout);
			if(!empty($name)) {
				$this->updateDashboardName(Piwik::getCurrentUserLogin(),$idDashboard, $name);
			}
		}
	}

	/**
	 * Saves the layout as default
	 */
	public function saveLayoutAsDefault()
	{
		$this->checkTokenInUrl();

		if(Piwik::isUserIsSuperUser()) {
			$layout      = Piwik_Common::unsanitizeInputValue(Piwik_Common::getRequestVar('layout'));
			$UserDashboard = Piwik_Db_Factory::getDAO('user_dashboard');
			$UserDashboard->saveLayout('', '1', $layout);
		}

	}

	/**
	 * Get the dashboard layout for the current user (anonymous or loggued user)
	 *
	 * @param int $idDashboard
	 * @return string $layout
	 */
	protected function getLayout($idDashboard)
	{
		if(Piwik::isUserIsAnonymous())
		{
			$session = new Piwik_Session_Namespace("Piwik_Dashboard");
			if(!isset($session->dashboardLayout))
			{
				return $this->getDefaultLayout();
			}
			$layout = $session->dashboardLayout;
		}
		else
		{
			$layout = $this->getLayoutForUser(Piwik::getCurrentUserLogin(), $idDashboard);
		}
		if(!empty($layout))
		{
			$layout = $this->removeDisabledPluginFromLayout($layout);
		}
		
		if ($layout === false)
		{
			$layout = $this->getDefaultLayout();
		}
		return $layout;
	}
	
	protected function removeDisabledPluginFromLayout($layout)
	{
		$layout = str_replace("\n", "", $layout);
		// if the json decoding works (ie. new Json format)
		// we will only return the widgets that are from enabled plugins
		$layoutObject = Piwik_Common::json_decode($layout, $assoc = false);

		if(is_array($layoutObject)) {
			$layoutObject = (object) array(
			    'config'  => array( 'layout' => '33-33-33' ),
			    'columns' => $layoutObject
			);
		}
		
		if(empty($layoutObject) || empty($layoutObject->columns))
		{
			$layoutObject = (object) array(
			    'config'  => array( 'layout' => '33-33-33' ),
			    'columns' => array()
			);
		}

		foreach($layoutObject->columns as &$row)
		{
			if(!is_array($row))
			{
				$row = array();
				continue;
			}

			foreach($row as $widgetId => $widget)
			{
				if(isset($widget->parameters->module)) {
					$controllerName = $widget->parameters->module;
					$controllerAction = $widget->parameters->action;
					if(!Piwik_IsWidgetDefined($controllerName, $controllerAction))
					{
						unset($row[$widgetId]);
					}
				}
				else
				{
					unset($row[$widgetId]);
				}
			}
		}
		$layout = Piwik_Common::json_encode($layoutObject);
		return $layout;
	}
	
	protected function getDefaultLayout()
	{
		$defaultLayout = $this->getLayoutForUser('', 1);

		if(empty($defaultLayout)) {
			$defaultLayout = '[
	            [
	                {"uniqueId":"widgetVisitsSummarygetEvolutionGraphcolumnsArray","parameters":{"module":"VisitsSummary","action":"getEvolutionGraph","columns":"nb_visits"}},
	                {"uniqueId":"widgetLivewidget","parameters":{"module":"Live","action":"widget"}},
	                {"uniqueId":"widgetVisitorInterestgetNumberOfVisitsPerVisitDuration","parameters":{"module":"VisitorInterest","action":"getNumberOfVisitsPerVisitDuration"}}
	            ],
	            [
	                {"uniqueId":"widgetReferersgetKeywords","parameters":{"module":"Referers","action":"getKeywords"}},
	                {"uniqueId":"widgetReferersgetWebsites","parameters":{"module":"Referers","action":"getWebsites"}}
	            ],
	            [
	                {"uniqueId":"widgetUserCountryMapworldMap","parameters":{"module":"UserCountryMap","action":"worldMap"}},
	                {"uniqueId":"widgetUserSettingsgetBrowser","parameters":{"module":"UserSettings","action":"getBrowser"}},
	                {"uniqueId":"widgetReferersgetSearchEngines","parameters":{"module":"Referers","action":"getSearchEngines"}},
	                {"uniqueId":"widgetVisitTimegetVisitInformationPerServerTime","parameters":{"module":"VisitTime","action":"getVisitInformationPerServerTime"}},
	                {"uniqueId":"widgetExampleRssWidgetrssPiwik","parameters":{"module":"ExampleRssWidget","action":"rssPiwik"}}
	            ]
	        ]';
		}
		$defaultLayout = $this->removeDisabledPluginFromLayout($defaultLayout);
		return $defaultLayout;
	}
	
	protected function getAvailableLayouts()
	{
	    return array(
	        array(100),
	        array(50,50), array(75,25), array(25,75),
	        array(33,33,33), array(50,25,25), array(25,50,25), array(25,25,50),
	        array(25,25,25,25)
	    );
	}

}


