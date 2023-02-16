package vn.vnpay.kafka;

import lombok.Getter;
import lombok.Setter;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Set;

@Getter
@Setter
public abstract class ObjectPool<T> {
    private final Hashtable<T, Long> locked;
    private final Hashtable<T, Long> unlocked;
    private long expirationTime;

    public ObjectPool() {
        expirationTime = 30000; // 30 seconds
        locked = new Hashtable<T, Long>();
        unlocked = new Hashtable<T, Long>();
    }
    public synchronized void shutdown(){
        unlocked.forEach((t, aLong) -> expire(t));
        locked.forEach((t, aLong) -> expire(t));
        unlocked.clear();
        locked.clear();
    }
    protected abstract T create();

    public abstract boolean validate(T o);

    public abstract void expire(T o);

    public synchronized T checkOut() {
        long now = System.currentTimeMillis();
        T t;
        if (unlocked.size() > 0) {
            Enumeration<T> e = unlocked.keys();
            while (e.hasMoreElements()) {
                t = e.nextElement();
                if ((now - unlocked.get(t)) > expirationTime) {
                    // object has expired
                    unlocked.remove(t);
                    expire(t);
                    t = null;
                } else {
                    if (validate(t)) {
                        unlocked.remove(t);
                        locked.put(t, now);
                        return (t);
                    } else {
                        // object failed validation
                        unlocked.remove(t);
                        expire(t);
                        t = null;
                    }
                }
            }
        }
        // no objects available, create a new one
        t = create();
        locked.put(t, now);
        return (t);
    }

    public synchronized void checkIn(T t) {
        locked.remove(t);
        unlocked.put(t, System.currentTimeMillis());
    }
}
