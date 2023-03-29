package bakemono

import (
	"math/rand"
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestDirSetGet(t *testing.T) {
	for i := 0; i < 10; i++ {
		testDirOnce(t)
	}
}

func testDirOnce(t *testing.T) {
	convey.Convey("Given a Dir", t, func() {
		dir := Dir{}

		convey.Convey("When setting various fields", func() {
			r := rand.New(rand.NewSource(42))
			expectedOffset := uint64(r.Uint32())
			expectedBig := uint8(r.Intn(4))
			expectedSize := uint8(r.Intn(64))
			expectedTag := uint16(r.Intn(4096))
			expectedPhase := r.Intn(2) == 1
			expectedHead := r.Intn(2) == 1
			expectedPinned := r.Intn(2) == 1
			expectedToken := r.Intn(2) == 1

			setters := []func(){
				func() { dir.setOffset(expectedOffset) },
				func() { dir.setBig(expectedBig) },
				func() { dir.setSize(expectedSize) },
				func() { dir.setTag(expectedTag) },
				func() { dir.setPhase(expectedPhase) },
				func() { dir.setHead(expectedHead) },
				func() { dir.setPinned(expectedPinned) },
				func() { dir.setToken(expectedToken) },
			}

			// Randomly shuffle the order of the setter functions.
			for i := range setters {
				j := rand.Intn(i + 1)
				setters[i], setters[j] = setters[j], setters[i]
			}

			// Apply the setter functions to the Dir instance in random order.
			for _, f := range setters {
				f()
			}

			convey.Convey("Then getting the same fields should return the expected values", func() {
				convey.So(dir.offset(), convey.ShouldEqual, expectedOffset)
				convey.So(dir.big(), convey.ShouldEqual, expectedBig)
				convey.So(dir.size(), convey.ShouldEqual, expectedSize)
				convey.So(dir.tag(), convey.ShouldEqual, expectedTag)
				convey.So(dir.phase(), convey.ShouldEqual, expectedPhase)
				convey.So(dir.head(), convey.ShouldEqual, expectedHead)
				convey.So(dir.pinned(), convey.ShouldEqual, expectedPinned)
				convey.So(dir.token(), convey.ShouldEqual, expectedToken)
			})
		})
	})
}
