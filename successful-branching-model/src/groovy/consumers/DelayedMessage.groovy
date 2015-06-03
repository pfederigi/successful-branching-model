package consumers

import org.apache.log4j.Logger

import java.util.concurrent.DelayQueue
import java.util.concurrent.Delayed
import java.util.concurrent.TimeUnit

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

/**
* Wrapper para almacenar un mensaje en la cola de espera
*/
class DelayedMessage implements Delayed {
    final long origin;
    final long delay;
    def key
    def message

    DelayedMessage(long delay, key, message) {
      this.origin = System.currentTimeMillis();
      this.delay = delay
      this.key = key
      this.message = message
    }

    @Override
    public long getDelay( TimeUnit unit ) {
        return unit.convert( delay - ( System.currentTimeMillis() - origin ), 
                TimeUnit.MILLISECONDS );
    }

    @Override
    public int compareTo( Delayed delayed ) {

        if( delayed instanceof DelayedMessage ) {
            long diff = origin - ( ( DelayedMessage )delayed ).origin;
            return ( ( diff == 0 ) ? 0 : ( ( diff < 0 ) ? -1 : 1 ) );
        }

        long d = ( getDelay( TimeUnit.MILLISECONDS ) - delayed.getDelay( TimeUnit.MILLISECONDS ) );
        return ( ( d == 0 ) ? 0 : ( ( d < 0 ) ? -1 : 1 ) );
    }

    @Override
    public int hashCode() {
        final int prime = 31;

        int result = 1;
        result = prime * result + ( ( key == null ) ? 0 : key.hashCode() );

        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if( this == obj ) {
            return true;
        }

        if( obj == null ) {
            return false;
        }

        if( !( obj instanceof DelayedMessage ) ) {
            return false;
        }

        final DelayedMessage other = ( DelayedMessage )obj;
        if( key == null ) {
            if( other.key != null ) {
                return false;
            }
        } else if( !key.equals( other.key ) ) {
            return false;
        }

        return true;
    }

}