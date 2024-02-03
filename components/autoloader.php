<?php

function myAutoloader($class) {
    $class = preg_replace('/^app/', __DIR__ . '/..', $class);
    include $class . ".php";
}

spl_autoload_register("myAutoloader");
