<?php

use Psr\Http\Message\ResponseInterface as Response;
use Psr\Http\Message\ServerRequestInterface as Request;
use Slim\Factory\AppFactory;

require __DIR__ . '/../vendor/autoload.php';

$app = AppFactory::create();

$app->setBasePath('/api');

$app->addErrorMiddleware(true, true, true);

$app->options('/{routes:.+}', function ($request, $response, $args) {
    return $response;
});

$app->add(function ($request, $handler) {
    $response = $handler->handle($request);
    return $response
        ->withHeader('Access-Control-Allow-Origin', '*')
        ->withHeader('Access-Control-Allow-Headers', 'X-Requested-With, Content-Type, Accept, Origin, Authorization')
        ->withHeader('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, PATCH, OPTIONS');
});

/**
 * GET /postindex
 *
 * Command for getting data from the table where store info about post index.
 *
 * Processed query parameters: page, limit, address
 * page - number page
 * limit - count records per page
 * address - search by address
 *
 * return
 * if success - json array with
 * [data=><data information>, totalPages=>'totalPages', currentPage=>'currentPage']
 * if error - error message {'error': 'error message'}
 *
 * @param Request $request The Slim Framework request object.
 * @param Response $response The Slim Framework response object.
 * @param array $args
 */
$app->get('/postindex', function (Request $request, Response $response, $args) {
    try {
        $lang = 'ukr';
        $limit = $request->getQueryParams()['limit'] ?? 50;
        $page = $request->getQueryParams()['page'] ?? 1;
        $offset = ($page - 1) * $limit;
        $address = $request->getQueryParams()['address'] ?? null;

        $db = \app\models\DBConnection::connect();
        $modelPostIndex = new \app\models\PostIndex($db);

        $mainSql = $modelPostIndex->getBaseSelect($lang);
        if ($address) {
            $mainSql .= "WHERE (";
            foreach ($modelPostIndex->getAddressFields($lang, false) as $field) {
                $mainSql .= "{$field} LIKE :address OR ";
            }
            $mainSql = substr($mainSql, 0, -3);
            $mainSql .= ") ";
            $mainSql .= " ORDER BY address ";
        } else {
            $mainSql .= " ORDER BY {$modelPostIndex->getFieldKey()} ";
        }
//        $response->getBody()->write($mainSql);
        //total records in result of query
        $sqlCount = "SELECT COUNT(*) FROM ( {$mainSql}) as tmp";
        $queryCount = $db->prepare($sqlCount);
        if ($address) {
            $queryCount->bindParam(':address', $address);
        }
        $queryCount->execute();
        $totalRecord = $queryCount->fetchColumn();

        //if page in query > total page than set page = total page
        if ((int)ceil($totalRecord / $limit) < $page) {
            $offset = (int)ceil($totalRecord / $limit) === 0 ? 0 : ((int)ceil($totalRecord / $limit) - 1) * $limit;
        }

//        $response->getBody()->write('total record '.$totalRecord.' totalPages'.(int)round($totalRecord / $limit).' offset '.$offset.' limit '.$limit.' page '.(int)($offset/$limit));
        $query = $db->prepare("SELECT * FROM ( {$mainSql}) as tmp LIMIT :limit OFFSET :offset ");
        $query->bindParam(':limit', $limit, PDO::PARAM_INT);
        $query->bindParam(':offset', $offset, PDO::PARAM_INT);
        if ($address) {
            $address = "%{$address}%";
            $query->bindParam(':address', $address);;
        }
        $query->execute();
        $payload = [
            'data' => $query->fetchAll(PDO::FETCH_ASSOC),
            'totalPages' => (int)ceil($totalRecord / $limit),
            'currentPage' => (int)($offset / $limit) + 1
        ];

    } catch (Exception $e) {
        $payload['error'] = $e->getMessage();
    } catch (\PDOException $e) {
        $payload['error'] = 'DB error: ' . $e->getMessage();
    }

    $payload = json_encode($payload);

    $response->getBody()->write($payload);

    return $response
        ->withHeader('Content-Type', 'application/json');
});


/**
 * GET /postindex/{post_office_id}
 *
 * Command for getting one record by value (post_office_id) of post index from the table where store info about post index.
 *
 * return
 * if success - json array with data
 * if error - error message {'error': 'error message'}
 *
 * @param Request $request The Slim Framework request object.
 * @param Response $response The Slim Framework response object.
 * @param array $args
 */
$app->get('/postindex/{post_office_id}', function (Request $request, Response $response, $args) {

    $lang = 'ukr';
    try {
        $db = \app\models\DBConnection::connect();

        $modelPostIndex = new \app\models\PostIndex($db);
        $mainSql = $modelPostIndex->getBaseSelect($lang);

        $query = $db->prepare($mainSql . " WHERE `{$modelPostIndex->getFieldKey()}` = :{$modelPostIndex->getFieldKey()}");
        $query->execute([":{$modelPostIndex->getFieldKey()}" => $args['post_office_id']]);
        $payload = $query->fetch(PDO::FETCH_ASSOC);
    } catch (Exception $e) {
        $payload['error'] = $e->getMessage();
    } catch (\PDOException $e) {
        $payload['error'] = 'DB error: ' . $e->getMessage();
    }

    $payload = json_encode($payload);

    $response->getBody()->write($payload);

    $response
        ->withHeader('Content-Type', 'application/json');
    return $response;
});


$app->post('/postindex/add', function (Request $request, Response $response) {
    try {
        $db = \app\models\DBConnection::connect();
        $modelPostIndex = new \app\models\PostIndex($db);

        $datas = $request->getParsedBody();
        if (array_key_exists('post_office_id', $datas)) {
            $sql = $modelPostIndex->generatePrepareInsertQuery();
            $query = $db->prepare($sql);
            $paramForQuery = [];
            foreach ($modelPostIndex->getAllFields() as $field => $value) {
                $paramForQuery[] = $datas[$field];
            }
            $query->execute($paramForQuery);
        }
        $payload = 'success';
    } catch (Exception $e) {
        $payload['error'] = $e->getMessage();
    } catch (\PDOException $e) {
        $payload['error'] = 'DB error: ' . $e->getMessage();
    }

    $payload = json_encode($payload);

    $response->getBody()->write($payload);

    $response
        ->withHeader('Content-Type', 'application/json');
    return $response;
});

$app->delete('/postindex/{post_office_id}', function (Request $request, Response $response, $args) {
    try {
        $db = \app\models\DBConnection::connect();
        $modelPostIndex = new \app\models\PostIndex($db);

        $query = $db->prepare("DELETE FROM `{$modelPostIndex->getTableName()}` WHERE `{$modelPostIndex->getFieldKey()}` = :{$modelPostIndex->getFieldKey()}");
        $query->execute([":{$modelPostIndex->getFieldKey()}" => $args['post_office_id']]);

        $payload = 'success';
    } catch (Exception $e) {
        $payload['error'] = $e->getMessage();
    } catch (\PDOException $e) {
        $payload['error'] = 'DB error: ' . $e->getMessage();
    }

    $payload = json_encode($payload);

    $response->getBody()->write($payload);

    $response
        ->withHeader('Content-Type', 'application/json');
    return $response;
});

$app->get('/{slug}', function (Request $request, Response $response, $args) {
    $response->getBody()->write('Wrong request');
    $response->withStatus(404);
    return $response;
});

$app->run();
