package com.kxw.observer;

import java.util.Observer;

public interface ZooKeeperObserver extends Observer {

    String getWatchedNode();

}
