import { pipelinePromise, Transform, Writable } from 'streamx'
import pDefer from 'p-defer'

/*
Goal:
Simulate data being dropped before drain

Looks like:
- Write to stream, see that it's not drained, wait for first drain
- Stream closes before drain
- Stream should emit close but never emit drain

*/

const pauseable = makePauseable()

const onDone = pipelinePromise(pauseable.stream, new Writable({
  write (chunk, cb) {
    console.log(`Log ${chunk}`)
    cb()
  }
}))

pauseable.stream.on('drain', () => console.log('drain'))
pauseable.stream.on('close', () => console.log('close'))

let drained = pauseable.stream.write('Hello')
console.log(drained)

setTimeout(() => {
  pauseable.toggle()
  drained = pauseable.stream.write('World')
  console.log(drained)

  setTimeout(() => {
    console.log('Destroying')
    pauseable.kill()
  })
})

await onDone.catch(() => console.log('It errored 🤷'))

function makePauseable () {
  let onResume = null
  let killed = false
  let hasListen = false

  const stream = new Transform({
    highWaterMark: 0,
    transform (chunk, cb) {
      if (killed) {
        this.push(null)
        cb(new Error('Killed'))
        return
      }
      if (onResume) {
        console.log(`Queue ${chunk}`)
        hasListen = true
        onResume.promise.then(() => {
          console.log(`Flush ${chunk}`)
          if (killed) {
            this.push(null)
            cb(new Error('Killed'))
            return
          }
          this.push(chunk)
          cb()
        }).catch(cb)
      } else {
        this.push(chunk)
        cb()
      }
    }
  })

  function kill () {
    killed = true
    if (onResume && hasListen) {
      onResume.reject(new Error('killed before flush'))
    } else stream.destroy()
  }
  function toggle () {
    hasListen = false
    if (!onResume) {
      // Pause
      onResume = pDefer()
    } else {
      // Unpause
      onResume.resolve()
      onResume = null
    }
  }

  return { stream, toggle, kill }
}
