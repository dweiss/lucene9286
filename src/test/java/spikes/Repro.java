package spikes;

import com.carrotsearch.progresso.Progress;
import com.carrotsearch.progresso.TaskStats;
import com.carrotsearch.progresso.Tracker;
import com.carrotsearch.progresso.views.console.ConsoleAware;
import org.apache.lucene.store.InputStreamDataInput;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.fst.CharSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FSTCompiler;
import org.apache.lucene.util.fst.IntsRefFSTEnum;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class Repro {
  static volatile Object hole;

  @Test
  public void recompileFst() throws IOException {
    try (InputStreamDataInput in =
        new InputStreamDataInput(
            new GZIPInputStream(
                getClass()
                    .getClassLoader()
                    .getResourceAsStream("fst-17291407798783309064.fst.gz")))) {
      try (Progress progress = new Progress(ConsoleAware.newConsoleProgressView())) {
        FST<CharsRef> originalFst;
        try (Tracker t = progress.newGenericSubtask("Reading FST").start()) {
          originalFst = new FST<>(in, CharSequenceOutputs.getSingleton());
        }

        for (float oversizingFactor : List.of(0f, 0.2f, 0.4f, 0.6f, 0.8f, 1f)) {
          FST<CharsRef> fst;
          try (Tracker t =
              progress
                  .newGenericSubtask("FST construction (of=" + oversizingFactor + ")")
                  .start()) {
            fst = recompile(originalFst, oversizingFactor);
            t.attribute("FST RAM", "%,d bytes", fst.ramBytesUsed());
            t.attribute("Oversizing factor", "%.2f", oversizingFactor);
          }
          try (Tracker t =
              progress.newGenericSubtask("TermEnum scan (of=" + oversizingFactor + ")").start()) {
            hole = walk(fst);
          }
        }
        System.out.println(TaskStats.breakdown(progress.tasks()));
      }
    }
  }

  private FST<CharsRef> recompile(FST<CharsRef> fst, float oversizingFactor) throws IOException {
    final FSTCompiler<CharsRef> builder =
        new FSTCompiler.Builder<>(
            FST.INPUT_TYPE.BYTE4,
            CharSequenceOutputs.getSingleton())
            .shouldShareSuffix(true)
            .shouldShareNonSingletonNodes(true)
            .shareMaxTailLength(Integer.MAX_VALUE)
            .allowFixedLengthArcs(true)
            .directAddressingMaxOversizingFactor(oversizingFactor)
        .build();

    IntsRefFSTEnum<CharsRef> i = new IntsRefFSTEnum<>(fst);
    IntsRefFSTEnum.InputOutput<CharsRef> c;
    while ((c = i.next()) != null) {
      builder.add(c.input, CharsRef.deepCopyOf(c.output));
    }
    return builder.compile();
  }

  private int walk(FST<CharsRef> read) throws IOException {
    IntsRefFSTEnum<CharsRef> i = new IntsRefFSTEnum<>(read);
    IntsRefFSTEnum.InputOutput<CharsRef> c;
    int cnt = 0;
    int terms = 0;
    while ((c = i.next()) != null) {
      terms += c.input.length;
      terms += c.output.length;
    }
    return terms;
  }
}
