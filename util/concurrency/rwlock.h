// @file rwlock.h generic reader-writer lock (cross platform support)

/*
 *    Copyright (C) 2010 10gen Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "mutex.h"
#include "../time_support.h"

// this requires Vista+ to work
// it works better than sharable_mutex under high contention
//#define MONGO_USE_SRW_ON_WINDOWS 1

#if !defined(MONGO_USE_SRW_ON_WINDOWS)

#if BOOST_VERSION >= 103500
# define BOOST_RWLOCK
#else
# if defined(_WIN32)
#  error need boost >= 1.35 for windows
# endif
# include <pthread.h>
#endif

#if defined(_WIN32)
# include "shared_mutex_win.hpp"
namespace mongo {
    typedef boost::modified_shared_mutex shared_mutex;
}
# undef assert
# define assert MONGO_assert
#elif defined(BOOST_RWLOCK)
# include <boost/thread/shared_mutex.hpp>
# undef assert
# define assert MONGO_assert
#endif

#endif

namespace mongo {

#if defined(MONGO_USE_SRW_ON_WINDOWS) && defined(_WIN32)

    class RWLock {
    public:
        RWLock(const char *, int lowPriorityWaitMS=0 ) { _lowPriorityWaitMS=lowPriorityWaitMS; InitializeSRWLock(&_lock); }
        ~RWLock() { }
        int lowPriorityWaitMS() const { return _lowPriorityWaitMS; }
        void lock()          { AcquireSRWLockExclusive(&_lock); }
        void unlock()        { ReleaseSRWLockExclusive(&_lock); }
        void lock_shared()   { AcquireSRWLockShared(&_lock); }
        void unlock_shared() { ReleaseSRWLockShared(&_lock); }
        bool lock_shared_try( int millis ) {
            unsigned long long end = curTimeMicros64() + millis*1000;
            while( 1 ) {
                if( TryAcquireSRWLockShared(&_lock) )
                    return true;
                if( curTimeMicros64() >= end )
                    break;
                Sleep(1);
            }
            return false;
        }
        bool lock_try( int millis = 0 ) {
            unsigned long long end = curTimeMicros64() + millis*1000;
            while( 1 ) {
                if( TryAcquireSRWLockExclusive(&_lock) )
                    return true;
                if( curTimeMicros64() >= end )
                    break;
                Sleep(1);
            }
            return false;
        }
    private:
        SRWLOCK _lock;
        int _lowPriorityWaitMS();
    };

#elif defined(BOOST_RWLOCK)
    class RWLock {
        shared_mutex _m;
        int _lowPriorityWaitMS;
    public:
#if defined(_DEBUG)
        const char *_name;
        RWLock(const char *name, int lowPriorityWait=0) : _lowPriorityWaitMS(lowPriorityWait) , _name(name) { }
#else
        RWLock(const char *, int lowPriorityWait=0) : _lowPriorityWaitMS(lowPriorityWait) { }
#endif

        int lowPriorityWaitMS() const { return _lowPriorityWaitMS; }

        void lock() {
            _m.lock();
#if defined(_DEBUG)
            mutexDebugger.entering(_name);
#endif
        }
        void unlock() {
#if defined(_DEBUG)
            mutexDebugger.leaving(_name);
#endif
            _m.unlock();
        }

        void lock_shared() {
            _m.lock_shared();
        }

        void unlock_shared() {
            _m.unlock_shared();
        }

        bool lock_shared_try( int millis ) {
            if( _m.timed_lock_shared( boost::posix_time::milliseconds(millis) ) ) {
                return true;
            }
            return false;
        }

        bool lock_try( int millis = 0 ) {
            if( _m.timed_lock( boost::posix_time::milliseconds(millis) ) ) {
#if defined(_DEBUG)
                mutexDebugger.entering(_name);
#endif
                return true;
            }
            return false;
        }


    };
#else
    class RWLock {
        pthread_rwlock_t _lock;
        int _lowPriorityWaitMS;
        
        inline void check( int x ) {
            if( x == 0 )
                return;
            log() << "pthread rwlock failed: " << x << endl;
            assert( x == 0 );
        }
        
    public:
#if defined(_DEBUG)
        const char *_name;
        RWLock(const char *name, int lowPriorityWaitMS=0) : _lowPriorityWaitMS(lowPriorityWaitMS), _name(name) {
            check( pthread_rwlock_init( &_lock , 0 ) );
        }
#else
        RWLock(const char *, int lowPriorityWaitMS=0) : _lowPriorityWaitMS( lowPriorityWaitMS ) {
            check( pthread_rwlock_init( &_lock , 0 ) );
        }
#endif
        

        ~RWLock() {
            if ( ! StaticObserver::_destroyingStatics ) {
                check( pthread_rwlock_destroy( &_lock ) );
            }
        }

        int lowPriorityWaitMS() const { return _lowPriorityWaitMS; }

        void lock() {
            check( pthread_rwlock_wrlock( &_lock ) );
#if defined(_DEBUG)
            mutexDebugger.entering(_name);
#endif
        }
        void unlock() {
#if defined(_DEBUG)
            mutexDebugger.leaving(_name);
#endif
            check( pthread_rwlock_unlock( &_lock ) );
        }

        void lock_shared() {
            check( pthread_rwlock_rdlock( &_lock ) );
        }

        void unlock_shared() {
            check( pthread_rwlock_unlock( &_lock ) );
        }

        bool lock_shared_try( int millis ) {
            return _try( millis , false );
        }

        bool lock_try( int millis = 0 ) {
            if( _try( millis , true ) ) {
#if defined(_DEBUG)
                mutexDebugger.entering(_name);
#endif
                return true;
            }
            return false;
        }

        bool _try( int millis , bool write ) {
            while ( true ) {
                int x = write ?
                        pthread_rwlock_trywrlock( &_lock ) :
                        pthread_rwlock_tryrdlock( &_lock );

                if ( x <= 0 ) {
                    return true;
                }

                if ( millis-- <= 0 )
                    return false;

                if ( x == EBUSY ) {
                    sleepmillis(1);
                    continue;
                }
                check(x);
            }

            return false;
        }

    };

#endif

    /** throws on failure to acquire in the specified time period. */
    class rwlock_try_write {
    public:
        struct exception { };
        rwlock_try_write(RWLock& l, int millis = 0) : _l(l) {
            if( !l.lock_try(millis) )
                throw exception();
        }
        ~rwlock_try_write() { _l.unlock(); }
    private:
        RWLock& _l;
    };

    /* scoped lock for RWLock */
    class rwlock {
    public:
        /**
         * @param lowPriority if > 0, will try to get the lock not gredily for that many # of ms
         */
        rwlock( const RWLock& lock , bool write , bool alreadyHaveLock = false , int lowPriorityWaitMS = 0 )
            : _lock( (RWLock&)lock ) , _write( write ) {
            
            if ( ! alreadyHaveLock ) {
                
                if ( _write ) {
                    
                    if ( ! lowPriorityWaitMS && lock.lowPriorityWaitMS() )
                        lowPriorityWaitMS = lock.lowPriorityWaitMS();
                    
                    if ( lowPriorityWaitMS ) { 
                        bool got = false;
                        for ( int i=0; i<lowPriorityWaitMS; i++ ) {  // we divide by 2 since we sleep a bit
                            if ( _lock.lock_try(0) ) {
                                got = true;
                                break;
                            }
                            sleepmillis(1);
                        }
                        if ( ! got ) {
                            log() << "couldn't get lazy rwlock" << endl;
                            _lock.lock();
                        }
                    }
                    else { 
                        _lock.lock();
                    }

                }
                else { 
                    _lock.lock_shared();
                }
            }
        }
        ~rwlock() {
            if ( _write )
                _lock.unlock();
            else
                _lock.unlock_shared();
        }
    private:
        RWLock& _lock;
        const bool _write;
    };
}
