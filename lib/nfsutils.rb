
module NFSUtils
  VERSION = '0.0.1'
  def version() NFSUtils::VERSION end

  require 'fileutils'
  require 'socket'
  require 'time'

  F = File
  Fu = FileUtils
  Pid = Process.pid
  Hostname = Socket.gethostname rescue 'localhost'
  Host = Hostname.gsub(%r/\./, '_')
  Program = $0

# base method for atomic nfs file operations
#
  def atomically(op, src, dst, opts = {})
    src_dirname, src_basename = F.split(src)
    src_stat = File.stat(src)

    dst = F.join(dst, src_basename) if F.directory?(dst)
    dst_dirname, dst_basename = F.split(dst)

    tmp = F.join(dst_dirname, ".#{ dst_basename }--#{ hostname }--#{ pid }--#{ tid }--#{ timestamp }--nfsutils.tmp")

    timeout = getopt(:timeout, opts, 42)
    utime = getopt(:utime, opts)
    mtime = getopt(:mtime, opts)
    atime = getopt(:atime, opts)

    safe_cp = lambda do |a, b|
      4.times do
        begin
          break(Fu.cp_r(a, b, :preserve => true))
        rescue => e
          STDERR.puts(errmsg(e))
          uncache(a) rescue nil
          uncache(b) rescue nil
          sleep(timeout)
        end
      end
    end

    safe_mv = lambda do |a, b|
      4.times do
        begin
          break(Fu.mv(a, b))
        rescue => e
          STDERR.puts(errmsg(e))
          uncache(a) rescue nil
          uncache(b) rescue nil
          sleep(timeout)
        end
      end
    end

    safe_rm = lambda do |a|
      4.times do
        begin
          break(Fu.rm_rf(a))
        rescue => e
          STDERR.puts(errmsg(e))
          uncache(a) rescue nil
          sleep(timeout)
        end
      end
    end

    begin
      case op.to_s
        when 'cp'
          safe_cp[src, tmp]
          safe_mv[tmp, dst]
        when 'mv'
          safe_cp[src, tmp]
          safe_mv[tmp, dst]
          safe_rm[src]
        else
          raise ArgumentError, op.to_s
      end
    ensure
      safe_rm[tmp]
    end

    if utime or mtime or atime
      a = (atime or utime or src_stat.atime)
      m = (mtime or utime or src_stat.mtime)
      a = src_stat.atime unless Time === a
      m = src_stat.mtime unless Time === m
# TODO
      # ALib::Util::find(dst){|e| File.utime(a, m, e)}
    end

    dst
  end


  def cp(*args, &block)
    atomically(:cp, *args, &block)
  end

  def mv(*args, &block)
    atomically(:mv, *args, &block)
  end
        

# make best effort to invalidate any inode caching done by nfs clients
#
  def uncache file 
    refresh = nil
    begin
      is_a_file = F === file
      path = (is_a_file ? file.path : file.to_s) 
      stat = (is_a_file ? file.stat : F.stat(file.to_s)) 
      refresh = tmpnam(F.dirname(path))
      ignoring_errors do
        F.link(path, refresh) rescue F.symlink(path, refresh)
      end
      ignoring_errors do
        F.chmod(stat.mode, path)
      end
      ignoring_errors do
        F.utime(stat.atime, stat.mtime, path)
      end
      ignoring_errors do
        open(F.dirname(path)){|d| d.fsync}
      end
    ensure 
      ignoring_errors do
        F.unlink(refresh) if refresh
      end
    end
  end

  def ignoring_errors(&block)
    begin
      block.call
    rescue Object => e
      warn errmsg(e) if $DEBUG
      e
    end
  end

# global tmpnams
#
  def tmpnam(*argv)
    opts = argv.last.is_a?(Hash) ? argv.pop : {}
    dirname = (argv.shift || getopt(%w(dir base prefix), opts, '.')).to_s
    seed = getopt(:seed, opts, Program) 
    reap = getopt(:reap, opts, false)

    dirname = File.expand_path(dirname)
    seed = seed.gsub(%r/[^0-9a-zA-Z]/, '_').gsub(%r/\s+/, '')

# TODO
=begin
    if reap
      begin
        baseglob =
          if nodot
            "%s__*__*__*__%s" % [ host, seed ] 
          else
            ".%s__*__*__*__%s" % [ host, seed ] 
          end
        host_re = 
          if nodot
            %r/^#{ host }$/
          else
            %r/^\.#{ host }$/
          end
        g = File.join dirname, baseglob
        Dir.glob(g).each do |candidate|
          basename = File.basename candidate
          parts = basename.split %r/__/, 5
          if parts[0] =~ host_re
            pid = Integer parts[1]
            unless alive? pid
              FileUtils.rm_rf candidate
            end
          end
        end
      rescue => e
        warn(errmsg(e)) rescue nil
      end
    end
=end

    basename =
      ".%s--%s--%s--%s--%s--%s.nfsutils" % [
        seed,
        hostname,
        pid,
        tid,
        timestamp,
        rand,
      ] 

    File.join(dirname, basename)
  end


# symbol/string key, nil/false value,  agnostic option getter
#
  def getopt opt, hash, default = nil
    keys = opt.respond_to?('each') ? opt : [opt]
    keys.each do |key|
      return hash[key] if hash.has_key? key
      key = "#{ key }"
      return hash[key] if hash.has_key? key
      key = key.intern
      return hash[key] if hash.has_key? key
    end
    return default
  end

# fomat errors like ruby does
#
  def errmsg e
    m, c, b = e.message, e.class, (e.backtrace||[])
    "#{ m } (#{ c })\n#{ b }"
  end

  def hostname
    Hostname
  end

  def pid
    Pid
  end

  def tid
    Thread.current.object_id.abs
  end

  def timestamp
    Time.now.utc.iso8601(4)
  end

  extend self
end
