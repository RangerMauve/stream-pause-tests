import { pipelinePromise, Readable, Transform, Writable } from 'streamx'
import pDefer from 'p-defer'

const controller = new AbortController()

const source = Readable.from([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], {
  signal: controller.signal
})

const delayer = makeDelayer(500)

const pausable = makePauseable()

setInterval(pausable.toggle, 1000)

setTimeout(() => {
  console.log('Destroying source')
  pausable.kill()
  controller.abort()
}, 4000)

function makeDelayer (delayTime, signal) {
  return new Transform({
    signal,
    transform (chunk, cb) {
      setTimeout(() => {
        this.push(chunk)
        cb()
      }, delayTime)
    }
  })
}

function makePauseable () {
  let onResume = null
  let killed = false

  function kill () {
    killed = true
    if (onResume) {
      onResume.reject(new Error('Killed while in progress'))
    }
  }
  function toggle () {
    if (!onResume) {
      // Pause
      onResume = pDefer()
    } else {
      // Unpause
      onResume.resolve()
      onResume = null
    }
  }

  const stream = new Transform({
    transform (chunk, cb) {
      if (killed) {
        this.push(null)
        cb()
        return
      }
      if (onResume) {
        console.log(`Queue ${chunk}`)
        onResume.promise.then(() => {
          console.log(`Flush ${chunk}`)
          if (killed) {
            this.push(null)
            cb()
            return
          }
          this.push(chunk)
          cb()
        }, () => {
          cb()
        })
      } else {
        this.push(chunk)
        cb()
      }
    }
  })

  return { stream, toggle, kill }
}

await pipelinePromise(
  source,
  delayer,
  pausable.stream,
  new Writable({
    write (chunk, cb) {
      console.log(`Log ${chunk}`)
      cb()
    }
  })
)

console.log('Done!')

process.exit(0)
