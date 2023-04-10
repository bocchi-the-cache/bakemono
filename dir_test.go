package bakemono

import (
	"math/rand"
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestDir_MarshalUnmarshalBinary(t *testing.T) {
	dir := Dir{}
	r := rand.New(rand.NewSource(42))
	expectedOffset := uint64(r.Uint32())
	expectedBig := uint8(r.Intn(4))
	expectedSize := uint8(r.Intn(64))
	expectedTag := uint16(r.Intn(4096))
	expectedPhase := r.Intn(2) == 1
	expectedHead := r.Intn(2) == 1
	expectedPinned := r.Intn(2) == 1
	expectedNext := uint16(r.Uint32())
	expectedApproxSize := uint64(r.Intn(DirMaxDataSize))
	//expectedPrev := uint16(r.Uint32())
	expectedToken := r.Intn(2) == 1

	dir.setOffset(expectedOffset)
	dir.setApproxSize(expectedApproxSize)
	//dir.setPrev(expectedPrev)
	dir.setBig(expectedBig)
	dir.setSize(expectedSize)
	dir.setTag(expectedTag)
	dir.setPhase(expectedPhase)
	dir.setHead(expectedHead)
	dir.setPinned(expectedPinned)
	dir.setToken(expectedToken)
	dir.setNext(expectedNext)

	buf, err := dir.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	dir2 := Dir{}
	err = dir2.UnmarshalBinary(buf)
	if err != nil {
		t.Fatal(err)
	}

	if dir2.offset() != expectedOffset {
		t.Fatalf("expected offset %d, got %d", expectedOffset, dir2.offset())
	}
	if dir2.approxSize() != expectedApproxSize {
		big := dir2.big()
		size := dir2.size()
		expectedApproxSize = (SectorSize << (big * 3)) * uint64(size+1)
		if dir2.approxSize() != expectedApproxSize {
			t.Fatalf("expected approxSize %d, got %d", expectedApproxSize, dir2.approxSize())
		}
	}
	//if dir2.prev() != expectedPrev {
	//	t.Fatalf("expected prev %d, got %d", expectedPrev, dir2.prev())
	//}
	if dir2.big() != expectedBig {
		t.Fatalf("expected big %d, got %d", expectedBig, dir2.big())
	}
	if dir2.size() != expectedSize {
		t.Fatalf("expected size %d, got %d", expectedSize, dir2.size())
	}
	if dir2.tag() != expectedTag {
		t.Fatalf("expected tag %d, got %d", expectedTag, dir2.tag())
	}
	if dir2.phase() != expectedPhase {
		t.Fatalf("expected phase %v, got %v", expectedPhase, dir2.phase())
	}
	if dir2.head() != expectedHead {
		t.Fatalf("expected head %v, got %v", expectedHead, dir2.head())
	}
	if dir2.pinned() != expectedPinned {
		t.Fatalf("expected pinned %v, got %v", expectedPinned, dir2.pinned())
	}
	if dir2.token() != expectedToken {
		t.Fatalf("expected token %v, got %v", expectedToken, dir2.token())
	}
	if dir2.next() != expectedNext {
		t.Fatalf("expected next %d, got %d", expectedNext, dir2.next())
	}

}

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
