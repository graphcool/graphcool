package util

import com.github.benmanes.caffeine.cache.{Caffeine, Cache => CaffeineCache}

import scala.collection.JavaConverters.{mapAsJavaMap, mapAsScalaMap}
import scala.collection.mutable
import scala.compat.java8.FunctionConverters._

object Cache {
  def unbounded[K, V >: Null](): Cache[K, V] = {
    val caffeineCache = Caffeine.newBuilder().asInstanceOf[Caffeine[K, V]].build[K, V]()
    Cache(caffeineCache)
  }

  def lfu[K, V >: Null](initialCapacity: Int, maxCapacity: Int): Cache[K, V] = {
    val caffeineCache = Caffeine
      .newBuilder()
      .initialCapacity(initialCapacity)
      .maximumSize(maxCapacity)
      .asInstanceOf[Caffeine[K, V]]
      .build[K, V]()
    Cache(caffeineCache)
  }
}

case class Cache[K, V >: Null](underlying: CaffeineCache[K, V]) {

  def get(key: K): Option[V] = Option(underlying.getIfPresent(key))

  def put(key: K, value: V): Unit = underlying.put(key, value)

  def remove(key: K): Unit = underlying.invalidate(key)

  def getOrUpdate(key: K, fn: () => V): V = {
    val caffeineFunction = (_: K) => fn()
    underlying.get(key, asJavaFunction(caffeineFunction))
  }

  def getOrUpdateOpt(key: K, fn: () => Option[V]): Option[V] = {
    val caffeineFunction = (_: K) => fn().orNull
    Option(underlying.get(key, asJavaFunction(caffeineFunction)))
  }

  def removeAll(fn: K => Boolean): Unit = {
    val keysToRemove: mutable.Map[K, V] = mapAsScalaMap(underlying.asMap()).filter((kv: (K, V)) => fn(kv._1))
    underlying.invalidateAll(mapAsJavaMap(keysToRemove).keySet())
  }

}
